package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.countVisibleChars
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocator
import io.github.kdroidfilter.seforimlibrary.core.models.BookVersion
import io.github.kdroidfilter.seforimlibrary.core.models.VersionLine
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.name
import kotlin.io.path.readText

/** קובץ מהדורה בלי actualLanguage — נפילה רועשת, בלי ניחוש שפה. */
internal class MissingVersionLanguageException(message: String) : IllegalStateException(message)

/**
 * Imports alternative book editions into book_version / version_line.
 *
 * Two deterministic sources, files always win:
 *  - Per-version sibling files (`<bookdir>/<versionTitle>.json`, produced by
 *    SefariaExport for multi-version titles): full text stored, hasContent=1.
 *    Each version's text is walked with the BOOK's schema (the exact merged
 *    walk) and joined to the book's lines by canonical ref.
 *  - merged.json's `versions` array otherwise: metadata-only rows, hasContent=0
 *    (single-version books — the book's own lines ARE the edition).
 *
 * Runs after all book lines are inserted, so every joined lineId exists.
 * Versions listed in black_versions.txt are skipped entirely (no row at all),
 * and so are version files whose actualLanguage isn't Hebrew.
 * Hebrew display names from version_titles_he.txt apply after the blacklist.
 */
internal class SefariaVersionsImporter(
    private val repository: SeforimRepository,
    private val allocator: IdAllocator,
    private val json: Json,
    private val payloadReader: SefariaBookPayloadReader,
    private val logger: Logger = Logger.withTag("SefariaVersionsImporter"),
    private val blacklist: VersionsBlacklist = VersionsBlacklist.Empty,
    hebrewTitles: VersionHebrewTitles = VersionHebrewTitles.Empty,
) {
    internal data class BookInput(
        val payload: BookPayload,
        val bookId: Long,
        val bookPath: String,
    )

    private var versionsWithContent = 0
    private var metadataOnlyVersions = 0
    private var versionLineRows = 0L
    private var segmentsUnmatched = 0L
    private var duplicateRefs = 0L
    private var emptyWalks = 0
    private var filesSkipped = 0
    private var versionsBlacklisted = 0
    private var versionsForeignLanguage = 0

    private val titlesUsage = VersionTitlesUsage(hebrewTitles, logger)

    private val versionBatch = mutableListOf<BookVersion>()
    private val lineBatch = mutableListOf<VersionLine>()

    suspend fun import(inputs: List<BookInput>, lineKeyToId: Map<Pair<String, Int>, Long>) {
        for (input in inputs) {
            val versionFiles = discoverVersionFiles(input.payload.sourceDirPath)
            if (versionFiles.isEmpty()) {
                importMetadataOnly(input)
            } else {
                importVersionFiles(input, versionFiles, lineKeyToId)
            }
            flushIfNeeded()
        }
        flush()
        logger.i {
            "Versions import: withContent=$versionsWithContent, metadataOnly=$metadataOnlyVersions, " +
                "versionLines=$versionLineRows, segmentsUnmatched=$segmentsUnmatched, " +
                "duplicateRefs=$duplicateRefs, emptyWalks=$emptyWalks, filesSkipped=$filesSkipped, " +
                "blacklisted=$versionsBlacklisted, foreignLanguage=$versionsForeignLanguage"
        }
        titlesUsage.logSummary()
    }

    // merged.json's `versions` pairs carry no language field at all, so these rows
    // are not language-gated; they are metadata-only (hasContent=0, no foreign text).
    private fun importMetadataOnly(input: BookInput) {
        input.payload.versionsMeta.distinctBy { it.title }.forEach { meta ->
            if (isBlacklisted(input.payload, meta.title, heVersionTitle = null)) return@forEach
            versionBatch += BookVersion(
                id = allocator.bookVersionId(input.bookId, meta.title),
                bookId = input.bookId,
                versionTitle = meta.title,
                heVersionTitle = titlesUsage.resolve(input.payload.heTitle, input.payload.enTitle, meta.title, null),
                versionSource = meta.source,
                hasContent = false,
            )
            metadataOnlyVersions++
        }
    }

    private suspend fun importVersionFiles(
        input: BookInput,
        versionFiles: List<Path>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
    ) {
        val payload = input.payload
        val schemaFile = payload.schemaFilePath?.let(Path::of)
        val schemaObj = schemaFile
            ?.let { runCatching { json.parseToJsonElement(it.readText()).jsonObject["schema"]?.jsonObject }.getOrNull() }
        if (schemaObj == null) {
            logger.w { "Versions of ${payload.enTitle} skipped: schema unreadable at ${payload.schemaFilePath}" }
            filesSkipped += versionFiles.size
            return
        }

        // ref -> lineId of the book's (merged) lines; first occurrence wins,
        // mirrored by the first-wins on the version-walk side below.
        val mergedLineIdByRef = HashMap<String, Long>(payload.refEntries.size)
        payload.refEntries.forEach { entry ->
            val lineId = lineKeyToId[input.bookPath to (entry.lineIndex - 1)] ?: return@forEach
            if (mergedLineIdByRef.putIfAbsent(entry.ref, lineId) != null) duplicateRefs++
        }

        for (file in versionFiles) {
            val doc = runCatching { json.parseToJsonElement(file.readText()).jsonObject }.getOrNull()
            if (doc == null) {
                logger.w { "Version file unparsable, skipped: $file" }
                filesSkipped++
                continue
            }
            val versionTitle = doc["versionTitle"]?.stringOrNull()?.trim()?.takeIf { it.isNotEmpty() }
            val textElement = doc["text"]
            if (versionTitle == null || textElement == null) {
                logger.w { "Version file missing versionTitle/text, skipped: $file" }
                filesSkipped++
                continue
            }
            // `language` הוא "he" בכל קובץ מהדורה בייצוא; השפה בפועל היא actualLanguage בלבד.
            val actualLanguage = doc["actualLanguage"]?.stringOrNull()?.trim()?.takeIf { it.isNotEmpty() }
                ?: throw MissingVersionLanguageException(
                    "actualLanguage חסר בקובץ המהדורה, ובלעדיו אי אפשר לקבוע את שפתה: $file. " +
                        "יש לתקן את הייצוא."
                )
            // עברית בלבד נכנסת ל-DB; כל שפה אחרת נחסמת, יידיש בכלל זה.
            if (actualLanguage != "he") {
                versionsForeignLanguage++
                logger.i { "Version skipped, actualLanguage=$actualLanguage: ${payload.heTitle} / $versionTitle" }
                continue
            }
            val heVersionTitle =
                doc["versionTitleInHebrew"]?.stringOrNull()?.trim()?.takeIf { it.isNotEmpty() }
            if (isBlacklisted(payload, versionTitle, heVersionTitle)) continue

            val walk = payloadReader.walkTextWithSchema(
                schemaObj = schemaObj,
                textElement = textElement,
                bookHeTitle = payload.heTitle,
                bookEnTitle = payload.enTitle,
                collectLineKeyOverrides = false,
            )
            val versionId = allocator.bookVersionId(input.bookId, versionTitle)
            val rows = ArrayList<VersionLine>(walk.refs.size)
            val seenRefs = HashSet<String>(walk.refs.size)
            walk.refs.forEach { ref ->
                if (!seenRefs.add(ref.ref)) {
                    duplicateRefs++
                    return@forEach
                }
                val lineId = mergedLineIdByRef[ref.ref]
                if (lineId == null) {
                    // טקסט ראשי ממהדורה מוצהרת אינו איחוד המהדורות — כתובת שאין לה
                    // שורה נופלת בקול, כי אין לאן לשמור אותה.
                    val preferredFile = payload.preferredVersionFileName
                    if (preferredFile != null) throw PreferredVersionRefGapException(
                        "preferred_versions.txt: ${payload.heTitle} נקרא מ-$preferredFile, " +
                            "ולמהדורה \"$versionTitle\" יש כתובת ללא שורה מקבילה: ${ref.ref}. " +
                            "לחסום את המהדורה ב-black_versions.txt או לתקן את המיפוי."
                    )
                    // A merged-based book keeps the original invariant (merged = union
                    // of versions); an unmatched address is counted, never guessed.
                    segmentsUnmatched++
                    return@forEach
                }
                val content = SefariaSameTractateRefs.strip(walk.lines[ref.lineIndex - 1], payload.sameTractateHeTitle)
                rows += VersionLine(
                    versionId = versionId,
                    lineId = lineId,
                    content = if (payload.boldDashDibburim) SefariaTalmudDibburBold.bold(content) else content,
                    charCount = countVisibleChars(content),
                )
            }
            if (rows.isEmpty()) {
                emptyWalks++
                logger.w { "Version walk yielded no lines: ${payload.enTitle} / $versionTitle" }
            }
            versionBatch += BookVersion(
                id = versionId,
                bookId = input.bookId,
                versionTitle = versionTitle,
                heVersionTitle = titlesUsage.resolve(payload.heTitle, payload.enTitle, versionTitle, heVersionTitle),
                versionSource = doc["versionSource"]?.stringOrNull(),
                priority = doc["priority"]?.stringOrNull()?.toDoubleOrNull(),
                license = doc["license"]?.stringOrNull(),
                versionNotes = doc["versionNotes"]?.stringOrNull(),
                heVersionNotes = doc["versionNotesInHebrew"]?.stringOrNull(),
                hasContent = rows.isNotEmpty(),
            )
            if (rows.isNotEmpty()) versionsWithContent++
            versionLineRows += rows.size
            lineBatch += rows
            // Flush per version file so lineBatch stays bounded even for books
            // with many large editions.
            flushIfNeeded()
        }
    }

    private fun isBlacklisted(payload: BookPayload, versionTitle: String, heVersionTitle: String?): Boolean {
        if (blacklist.isEmpty()) return false
        val bookKeys = setOfNotNull(normalizeTitleKey(payload.heTitle), normalizeTitleKey(payload.enTitle))
        val versionKeys = setOfNotNull(normalizeTitleKey(versionTitle), normalizeTitleKey(heVersionTitle))
        if (!blacklist.isBlocked(bookKeys, versionKeys)) return false
        versionsBlacklisted++
        logger.i { "Version blacklisted: ${payload.heTitle} / $versionTitle" }
        return true
    }

    private fun discoverVersionFiles(sourceDirPath: String?): List<Path> {
        val dir = sourceDirPath?.let(Path::of) ?: return emptyList()
        if (!Files.isDirectory(dir)) return emptyList()
        return Files.list(dir).use { stream ->
            stream.filter { Files.isRegularFile(it) }
                .filter { it.fileName.name.endsWith(".json", ignoreCase = true) }
                .filter { !it.fileName.name.equals("merged.json", ignoreCase = true) }
                .sorted()
                .toList()
        }
    }

    private suspend fun flushIfNeeded() {
        // book_version rows always land before their version_line rows.
        if (versionBatch.size >= SefariaImportTuning.LINE_BATCH_SIZE ||
            lineBatch.size >= SefariaImportTuning.LINE_BATCH_SIZE
        ) {
            flush()
        }
    }

    private suspend fun flush() {
        if (versionBatch.isNotEmpty()) {
            repository.insertBookVersionsBatch(versionBatch)
            versionBatch.clear()
        }
        if (lineBatch.isNotEmpty()) {
            repository.insertVersionLinesBatch(lineBatch)
            lineBatch.clear()
        }
    }
}
