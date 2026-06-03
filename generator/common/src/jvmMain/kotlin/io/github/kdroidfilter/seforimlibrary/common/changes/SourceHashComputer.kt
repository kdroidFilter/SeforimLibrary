package io.github.kdroidfilter.seforimlibrary.common.changes

import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookKey
import io.github.kdroidfilter.seforimlibrary.common.buildstate.BookSourceHash
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest

/**
 * Computes a canonical sha256 hash per book from its source artefact.
 *
 * The hash is opaque — its only contract is "identical input ⇒ identical
 * hash"; it has no meaning beyond detecting *whether* a book's source has
 * changed since the previous build. See `DELTA_UPDATE_PLAN.md` §6.2.
 *
 * Implementations:
 * - [SefariaSourceHashComputer] : hashes `merged.json` + accompanying schema.
 * - [OtzariaSourceHashComputer] : reads from `files_manifest.json` (per-file
 *   sha256 already provided by the upstream export).
 */
interface SourceHashComputer {

    /**
     * Walks the source tree under [root] and emits one entry per discovered
     * book. The [BookKey] follows the natural key used by [io.github.
     * kdroidfilter.seforimlibrary.common.ids.IdAllocator.bookId].
     */
    fun compute(root: Path, version: Int): Map<BookKey, BookSourceHash>
}

/**
 * Sefaria implementation: walks `json/<...>/<book>/merged.json` and hashes
 * `merged.json` bytes plus the matching `schemas/<title>.json` if present
 * (the schema influences the rendered output and so contributes to the
 * "did the source change?" signal).
 *
 * The book natural key is `(sourceName="Sefaria", canonicalHeTitle=<title from merged.json>)`
 * to match `SefariaDirectImporter.canonicalHeTitle`.
 */
class SefariaSourceHashComputer(
    private val sourceName: String = "Sefaria",
    private val titleExtractor: (Path) -> String? = ::defaultExtractHeTitle,
) : SourceHashComputer {

    override fun compute(root: Path, version: Int): Map<BookKey, BookSourceHash> {
        val jsonDir = root.resolve("json")
        val schemaDir = root.resolve("schemas")
        require(Files.isDirectory(jsonDir)) { "Missing json/ under $root" }

        val out = HashMap<BookKey, BookSourceHash>()
        Files.walk(jsonDir).use { stream ->
            stream
                .filter { Files.isRegularFile(it) && it.fileName.toString().equals("merged.json", ignoreCase = true) }
                .forEach { mergedPath ->
                    val heTitle = titleExtractor(mergedPath) ?: return@forEach
                    val md = MessageDigest.getInstance("SHA-256")
                    md.update(Files.readAllBytes(mergedPath))
                    val schemaPath = schemaDir.resolve(sanitizeSchemaFilename(heTitle) + ".json")
                    if (Files.exists(schemaPath)) md.update(Files.readAllBytes(schemaPath))
                    out[BookKey(sourceName, heTitle)] = BookSourceHash(md.digest(), version)
                }
        }
        return out
    }

    companion object {
        /**
         * Pulls `heTitle` directly from the merged.json header. We avoid a
         * full JSON parse to keep this fast; merged.json always starts with a
         * top-level object whose key order is stable in the upstream export.
         */
        fun defaultExtractHeTitle(mergedJson: Path): String? {
            val text = Files.readString(mergedJson)
            val heTitleMatch = Regex(""""heTitle"\s*:\s*"((?:\\.|[^"\\])*)"""").find(text)
                ?: return null
            return heTitleMatch.groupValues[1]
                // Unescape minimal JSON escapes that show up in Hebrew titles
                .replace("\\\"", "\"").replace("\\\\", "\\")
        }

        private fun sanitizeSchemaFilename(title: String): String =
            title.replace(' ', '_').replace("/", "_")
    }
}

/**
 * Otzaria implementation: reads `files_manifest.json` and exposes its
 * pre-computed sha256 per file. The natural key is
 * `(sourceName=<resolved manifest source>, canonicalHeTitle=<filename without ext>)`.
 *
 * For the Phase-2 detector we don't need to mimic the importer's actual
 * source resolution; the manifest's sha256 already pinpoints exactly the
 * touched files. We always emit the file's title (filename minus extension)
 * as the natural key — the touched-book detector compares against the
 * BookKey stored by the importer at the previous build's snapshot.
 */
class OtzariaSourceHashComputer(
    private val sourceNameResolver: (Path) -> String = { "Otzaria" },
) : SourceHashComputer {

    override fun compute(root: Path, version: Int): Map<BookKey, BookSourceHash> {
        val manifest = root.resolve("files_manifest.json")
        require(Files.isRegularFile(manifest)) { "Missing files_manifest.json under $root" }

        val text = Files.readString(manifest)
        val regex = Regex(""""([^"\\]*\.(?:txt|json))"\s*:\s*"([0-9a-fA-F]{64})"""")
        val out = HashMap<BookKey, BookSourceHash>()
        regex.findAll(text).forEach { m ->
            val relPath = m.groupValues[1]
            val sha256Hex = m.groupValues[2]
            if (!relPath.endsWith(".txt")) return@forEach
            val title = relPath.substringAfterLast('/').substringBeforeLast('.')
            if (title.isBlank()) return@forEach
            val source = sourceNameResolver(root.resolve(relPath))
            val hash = ByteArray(32) { i ->
                val hi = Character.digit(sha256Hex[i * 2], 16)
                val lo = Character.digit(sha256Hex[i * 2 + 1], 16)
                ((hi shl 4) or lo).toByte()
            }
            out[BookKey(source, title)] = BookSourceHash(hash, version)
        }
        return out
    }
}
