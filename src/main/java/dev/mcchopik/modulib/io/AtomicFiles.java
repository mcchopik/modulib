package dev.mcchopik.modulib.io;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public final class AtomicFiles {

    private AtomicFiles() {}

    public static void copy(Path source, Path target) throws IOException {
        Path normalizedSource = source.toAbsolutePath().normalize();
        Path normalizedTarget = target.toAbsolutePath().normalize();
        if (normalizedSource.equals(normalizedTarget)) return;

        Path parent = normalizedTarget.getParent();
        if (parent == null) throw new IOException("Target has no parent: " + normalizedTarget);
        Files.createDirectories(parent);
        Path temporary = Files.createTempFile(parent, "." + normalizedTarget.getFileName(), ".part");
        try {
            Files.copy(normalizedSource, temporary, StandardCopyOption.REPLACE_EXISTING);
            replace(temporary, normalizedTarget);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    public static void replace(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
