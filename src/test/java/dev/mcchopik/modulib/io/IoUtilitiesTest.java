package dev.mcchopik.modulib.io;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IoUtilitiesTest {

    @TempDir
    Path directory;

    @Test
    void hashesAndAtomicallyReplacesFiles() throws Exception {
        Path source = directory.resolve("source.bin");
        Path target = directory.resolve("nested/target.bin");
        Files.writeString(source, "abc");

        AtomicFiles.copy(source, target);

        assertEquals("abc", Files.readString(target));
        assertEquals("a9993e364706816aba3e25717850c26c9cd0d89d", Hashes.sha1(target));
    }
}
