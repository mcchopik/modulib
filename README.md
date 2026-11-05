# fabric-client-core

Reusable Java utilities for Fabric client mods. The library has no Minecraft dependency and can be embedded into a mod JAR.

## Modules

- `ParallelFileDownloader`: bounded parallel HTTP downloads with one aggregate bandwidth limit.
- `Hashes`: streaming SHA-1/SHA-256 helpers.
- `AtomicFiles`: same-directory temporary files and atomic replacement with a portable fallback.

## Build

Every push to `main` is built and tested by GitHub Actions. The binary and sources JARs are attached to the workflow run.

Java 21 or newer is required. The project is available under the MIT License.
