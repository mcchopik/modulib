package dev.mcchopik.modulib.net;

import dev.mcchopik.modulib.io.AtomicFiles;
import dev.mcchopik.modulib.io.Hashes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.concurrent.locks.LockSupport;

/** Parallel HTTP downloads with checksum, size, redirect and aggregate bandwidth controls. */
public final class ParallelFileDownloader {

    private static final Pattern SHA1 = Pattern.compile("^[0-9a-fA-F]{40}$");
    private static final int MAX_REDIRECTS = 5;

    private ParallelFileDownloader() {}

    public record Request<K>(K id, URL url, String expectedSha1, Path targetDirectory) {
        public Request {
            if (id == null || url == null || targetDirectory == null) {
                throw new IllegalArgumentException("Download id, URL and target directory are required");
            }
            if (expectedSha1 != null && !expectedSha1.isBlank() && !SHA1.matcher(expectedSha1).matches()) {
                throw new IllegalArgumentException("Malformed SHA-1: " + expectedSha1);
            }
        }
    }

    public record Options(int maxConcurrency, long maxBytes, long bytesPerSecond,
                          int connectTimeoutMs, int readTimeoutMs, int attempts,
                          boolean allowHttp, Proxy proxy, Map<String, String> headers) {
        public Options {
            maxConcurrency = Math.max(1, maxConcurrency);
            maxBytes = Math.max(1, maxBytes);
            bytesPerSecond = Math.max(1, bytesPerSecond);
            connectTimeoutMs = Math.max(1_000, connectTimeoutMs);
            readTimeoutMs = Math.max(1_000, readTimeoutMs);
            attempts = Math.max(1, attempts);
            proxy = proxy == null ? Proxy.NO_PROXY : proxy;
            headers = headers == null ? Map.of() : Map.copyOf(headers);
        }
    }

    public interface Listener<K> {
        default void started(K id, long expectedBytes) {}
        default void progressed(K id, long downloadedBytes) {}
        default void finished(K id, boolean success) {}
    }

    public record BatchResult<K>(Map<K, Path> downloaded, Map<K, Throwable> failed) {}

    /** Blocks the calling worker while requests run on a bounded set of child workers. */
    public static <K> BatchResult<K> download(Collection<Request<K>> requests, Options options,
                                               Listener<K> listener) throws InterruptedException {
        List<Request<K>> pending = List.copyOf(requests);
        validateUniqueIds(pending);
        Listener<K> safeListener = listener == null ? new Listener<>() {} : listener;
        int workers = Math.min(options.maxConcurrency(), Math.max(1, pending.size()));
        ExecutorService executor = Executors.newFixedThreadPool(workers, runnable -> {
            Thread thread = new Thread(runnable, "fabric-core-download");
            thread.setDaemon(true);
            return thread;
        });
        BatchContext context = new BatchContext(options.bytesPerSecond());
        List<Future<OneResult<K>>> futures = new ArrayList<>();
        try {
            ExecutorCompletionService<OneResult<K>> completions = new ExecutorCompletionService<>(executor);
            int submitted = 0;
            int completed = 0;
            while (submitted < Math.min(workers, pending.size())) {
                futures.add(completions.submit(task(pending.get(submitted++), options, safeListener, context)));
            }

            Map<K, Path> downloaded = new LinkedHashMap<>();
            Map<K, Throwable> failed = new LinkedHashMap<>();
            while (completed < pending.size()) {
                OneResult<K> result;
                try {
                    result = completions.take().get();
                } catch (java.util.concurrent.ExecutionException e) {
                    throw new IllegalStateException("Download worker failed", e.getCause());
                }
                completed++;
                if (result.path() == null) failed.put(result.id(), result.error());
                else downloaded.put(result.id(), result.path());
                if (submitted < pending.size()) {
                    futures.add(completions.submit(task(pending.get(submitted++), options, safeListener, context)));
                }
            }
            return new BatchResult<>(Map.copyOf(downloaded), Map.copyOf(failed));
        } catch (InterruptedException e) {
            context.cancel();
            futures.forEach(future -> future.cancel(true));
            throw e;
        } finally {
            executor.shutdownNow();
            context.cancel();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static <K> java.util.concurrent.Callable<OneResult<K>> task(
            Request<K> request, Options options, Listener<K> listener, BatchContext context) {
        return () -> downloadWithRetries(request, options, listener, context);
    }

    private static <K> OneResult<K> downloadWithRetries(Request<K> request, Options options,
                                                         Listener<K> listener, BatchContext context) {
        Throwable last = null;
        for (int attempt = 1; attempt <= options.attempts(); attempt++) {
            try {
                context.checkCancelled();
                Path path = downloadOnce(request, options, listener, context);
                notifyFinished(listener, request.id(), true);
                return new OneResult<>(request.id(), path, null);
            } catch (InterruptedException | CancellationException e) {
                Thread.currentThread().interrupt();
                notifyFinished(listener, request.id(), false);
                return new OneResult<>(request.id(), null, e);
            } catch (IOException | RuntimeException e) {
                last = e;
                if (attempt == options.attempts() || !isTransient(e)) break;
                try {
                    Thread.sleep(Math.min(2_000L, 200L << Math.min(3, attempt - 1)));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    last = interrupted;
                    break;
                }
            }
        }
        notifyFinished(listener, request.id(), false);
        return new OneResult<>(request.id(), null, last == null ? new IOException("Download failed") : last);
    }

    private static <K> Path downloadOnce(Request<K> request, Options options, Listener<K> listener,
                                         BatchContext context) throws IOException, InterruptedException {
        String expected = normalizeSha1(request.expectedSha1());
        Files.createDirectories(request.targetDirectory());
        Path target = expected == null ? null : request.targetDirectory().resolve(expected);
        if (target != null && Files.isRegularFile(target)
                && Files.size(target) <= options.maxBytes() && Hashes.sha1(target).equals(expected)) {
            notifyStarted(listener, request.id(), Files.size(target));
            return target;
        }

        ConnectionResult opened = open(request.url(), options, context);
        HttpURLConnection connection = opened.connection();
        Path temporary = Files.createTempFile(request.targetDirectory(), "download-", ".part");
        try {
            long contentLength = connection.getContentLengthLong();
            if (contentLength > options.maxBytes()) throw new PermanentDownloadException("File exceeds size limit");
            notifyStarted(listener, request.id(), contentLength);

            long bytes = 0;
            try (InputStream input = connection.getInputStream();
                 OutputStream output = Files.newOutputStream(temporary,
                         StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                byte[] buffer = new byte[8 * 1024];
                int read;
                while ((read = input.read(buffer)) != -1) {
                    context.checkCancelled();
                    bytes += read;
                    if (bytes > options.maxBytes()) throw new PermanentDownloadException("File exceeds size limit");
                    context.rateLimiter().acquire(read);
                    output.write(buffer, 0, read);
                    notifyProgressed(listener, request.id(), bytes);
                }
            }

            String actual = Hashes.sha1(temporary);
            if (expected != null && !expected.equals(actual)) throw new IOException("SHA-1 mismatch");
            context.checkCancelled();
            Path finalTarget = request.targetDirectory().resolve(expected == null ? actual : expected);
            AtomicFiles.replace(temporary, finalTarget);
            return finalTarget;
        } finally {
            context.remove(connection);
            connection.disconnect();
            Files.deleteIfExists(temporary);
        }
    }

    private static ConnectionResult open(URL original, Options options, BatchContext context) throws IOException {
        URL current = original;
        URI originalOrigin = origin(original);
        for (int redirect = 0; redirect <= MAX_REDIRECTS; redirect++) {
            validateScheme(current, options.allowHttp());
            HttpURLConnection connection = (HttpURLConnection) current.openConnection(options.proxy());
            context.add(connection);
            int status;
            try {
                connection.setInstanceFollowRedirects(false);
                connection.setConnectTimeout(options.connectTimeoutMs());
                connection.setReadTimeout(options.readTimeoutMs());
                if (origin(current).equals(originalOrigin)) {
                    options.headers().forEach(connection::setRequestProperty);
                } else {
                    options.headers().forEach((name, value) -> {
                        if (name.equalsIgnoreCase("User-Agent")) connection.setRequestProperty(name, value);
                    });
                }
                status = connection.getResponseCode();
            } catch (IOException | RuntimeException e) {
                context.remove(connection);
                connection.disconnect();
                throw e;
            }
            if (status >= 300 && status <= 399) {
                String location = connection.getHeaderField("Location");
                context.remove(connection);
                connection.disconnect();
                if (location == null || redirect == MAX_REDIRECTS) {
                    throw new PermanentDownloadException("Invalid redirect");
                }
                current = new URL(current, location);
                continue;
            }
            if (status < 200 || status >= 300) {
                context.remove(connection);
                connection.disconnect();
                throw new HttpStatusException(status);
            }
            return new ConnectionResult(connection);
        }
        throw new PermanentDownloadException("Too many redirects");
    }

    private static URI origin(URL url) throws IOException {
        try {
            int port = url.getPort() >= 0 ? url.getPort() : url.getDefaultPort();
            return new URI(url.getProtocol().toLowerCase(Locale.ROOT), null,
                    url.getHost().toLowerCase(Locale.ROOT), port, null, null, null);
        } catch (Exception e) {
            throw new IOException("Invalid URL origin", e);
        }
    }

    private static void validateScheme(URL url, boolean allowHttp) throws IOException {
        String protocol = url.getProtocol();
        if (!"https".equalsIgnoreCase(protocol) && !(allowHttp && "http".equalsIgnoreCase(protocol))) {
            throw new PermanentDownloadException("Unsupported URL scheme: " + protocol);
        }
        if (url.getHost() == null || url.getHost().isBlank()) {
            throw new PermanentDownloadException("URL has no host");
        }
    }

    private static boolean isTransient(Throwable error) {
        if (error instanceof PermanentDownloadException || error instanceof IllegalArgumentException) return false;
        if (error instanceof HttpStatusException status) {
            return status.status == 408 || status.status == 429 || status.status >= 500;
        }
        return error instanceof IOException;
    }

    private static String normalizeSha1(String hash) {
        return hash == null || hash.isBlank() ? null : hash.toLowerCase(Locale.ROOT);
    }

    private static <K> void validateUniqueIds(List<Request<K>> requests) {
        Set<K> ids = new HashSet<>();
        for (Request<K> request : requests) {
            if (!ids.add(request.id())) throw new IllegalArgumentException("Duplicate download id: " + request.id());
        }
    }

    private static <K> void notifyStarted(Listener<K> listener, K id, long bytes) {
        try { listener.started(id, bytes); } catch (RuntimeException ignored) {}
    }

    private static <K> void notifyProgressed(Listener<K> listener, K id, long bytes) {
        try { listener.progressed(id, bytes); } catch (RuntimeException ignored) {}
    }

    private static <K> void notifyFinished(Listener<K> listener, K id, boolean success) {
        try { listener.finished(id, success); } catch (RuntimeException ignored) {}
    }

    private record OneResult<K>(K id, Path path, Throwable error) {}
    private record ConnectionResult(HttpURLConnection connection) {}

    private static final class HttpStatusException extends IOException {
        private final int status;
        private HttpStatusException(int status) {
            super("HTTP " + status);
            this.status = status;
        }
    }

    private static final class PermanentDownloadException extends IOException {
        private PermanentDownloadException(String message) {
            super(message);
        }
    }

    private static final class BatchContext {
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final Set<HttpURLConnection> connections = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private final AggregateRateLimiter rateLimiter;

        private BatchContext(long bytesPerSecond) {
            this.rateLimiter = new AggregateRateLimiter(bytesPerSecond);
        }

        private void add(HttpURLConnection connection) {
            connections.add(connection);
        }

        private void remove(HttpURLConnection connection) {
            connections.remove(connection);
        }

        private void cancel() {
            cancelled.set(true);
            connections.forEach(HttpURLConnection::disconnect);
        }

        private void checkCancelled() throws InterruptedException {
            if (cancelled.get() || Thread.currentThread().isInterrupted()) throw new InterruptedException();
        }

        private AggregateRateLimiter rateLimiter() {
            return rateLimiter;
        }
    }

    private static final class AggregateRateLimiter {
        private final long bytesPerSecond;
        private long nextSlotNanos = System.nanoTime();

        private AggregateRateLimiter(long bytesPerSecond) {
            this.bytesPerSecond = bytesPerSecond;
        }

        private void acquire(int bytes) throws InterruptedException {
            if (Thread.interrupted()) throw new InterruptedException();
            long end;
            synchronized (this) {
                long now = System.nanoTime();
                long start = Math.max(now, nextSlotNanos);
                long duration = Math.max(1L, (long) ((bytes * 1_000_000_000.0) / bytesPerSecond));
                end = start + duration;
                nextSlotNanos = end;
            }
            long wait;
            while ((wait = end - System.nanoTime()) > 0) {
                LockSupport.parkNanos(wait);
                if (Thread.interrupted()) throw new InterruptedException();
            }
        }
    }
}
