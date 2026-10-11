package io.github.kdroidfilter.seforimlibrary.otzariasqlite

import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import kotlin.io.path.extension

/**
 * Writes `files_manifest.json` with the sha256 of every book under [sourceDir]/אוצריא, as the
 * library release ships it. A build records these source hashes, and the next build reads from
 * them which books the previous one had (book-id continuity).
 */
internal fun writeFilesManifest(sourceDir: Path) {
    val library = sourceDir.resolve("אוצריא")
    val entries = Files.walk(library).use { s ->
        s.filter { Files.isRegularFile(it) && it.extension == "txt" }.sorted().toList()
    }.map { file ->
        val sha = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)).joinToString("") { "%02x".format(it) }
        val key = "אוצריא/" + library.relativize(file).toString().replace('\\', '/')
        "  \"$key\": { \"hash\": \"$sha\" }"
    }
    Files.writeString(sourceDir.resolve("files_manifest.json"), entries.joinToString(",\n", "{\n", "\n}"))
}
