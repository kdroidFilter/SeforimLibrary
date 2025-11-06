package io.github.kdroidfilter.seforimlibrary.generator.sefaria

import co.touchlab.kermit.Logger
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.isRegularFile
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.math.max

/**
 * Builds an Otzaria-shaped source tree (אוצריא + links + metadata.json) from
 * Sefaria export folders (json + export_schemas + export_toc + export_links).
 */
class SefariaToOtzariaBuilder(private val logger: Logger) {
    private val json = Json { ignoreUnknownKeys = true; coerceInputValues = true }

    data class TocBook(
        val heTitle: String,
        val enTitle: String,
        val hePath: List<String>
    )

    private data class IndexedText(
        val enTitle: String,
        val textPath: Path,     // .../json/.../<Book>/Hebrew/merged.json
        val schemaPath: Path?   // export_schemas/schemas/<Normalized>.json (if found)
    )

    fun build(sefariaRoot: Path, outRoot: Path): Path {
        val toc = loadToc(sefariaRoot)
        val indexByTitle = indexTexts(sefariaRoot)
        // enTitle (normalized) -> heTitle
        val enToHe = toc.associate { normalizeTitle(it.enTitle) to (it.heTitle.ifBlank { it.enTitle }) }
        val schemasRoot = sefariaRoot.resolve("export_schemas").resolve("schemas")
        val linksRoot = sefariaRoot.resolve("export_links").resolve("links")

        val otzariaRoot = outRoot
        val libRoot = otzariaRoot.resolve("אוצריא")
        val linksOut = otzariaRoot.resolve("links")
        val metadataOut = otzariaRoot.resolve("metadata.json")
        Files.createDirectories(libRoot)
        Files.createDirectories(linksOut)

        // Collect metadata entries as we write books
        val metadataEntries = mutableListOf<Map<String, Any?>>()

        var written = 0
        for (book in toc) {
            val idx = indexByTitle[normalizeTitle(book.enTitle)] ?: continue
            // Load schema JSON to pull descriptive fields
            val schemaJson = idx.schemaPath?.let { runCatching { json.parseToJsonElement(it.readText()) }.getOrNull() } as? JsonObject
            val heTitle = book.heTitle.ifBlank { schemaJson?.get("heTitle")?.jsonPrimitive?.contentOrNull ?: book.enTitle }
            val outDir = libRoot
                .resolve(book.hePath.joinToString("/") { sanitizeFilename(it) })
                .resolve(sanitizeFilename(heTitle))
            outDir.createDirectories()

            // Build content lines from Sefaria JSON and schema
            val content = buildContent(idx.textPath, schemaJson)
            val outTxt = outDir.resolve("${sanitizeFilename(heTitle)}.txt")
            Files.writeString(outTxt, content.joinToString(separator = ""))

            // Minimal metadata row compatible with DatabaseGenerator.BookMetadata
            val meta = mutableMapOf<String, Any?>()
            meta["title"] = heTitle
            meta["author"] = (schemaJson?.get("authors") as? JsonArray)
                ?.firstOrNull()?.jsonObject?.get("he")?.jsonPrimitive?.contentOrNull
            meta["heShortDesc"] = asString(schemaJson?.get("heShortDesc"))
            meta["pubDate"] = asString(schemaJson?.get("pubDate"))
            meta["pubPlace"] = asString(schemaJson?.get("pubPlace"))
            metadataEntries.add(meta)

            // Try to produce per-book links from Sefaria CSVs (best-effort)
            // Strategy: pick rows where Text 1 == enTitle or Text 2 == enTitle and write a book-specific JSON
            runCatching { exportLinksForBook(linksRoot, linksOut, book.enTitle, heTitle, enToHe) }
                .onFailure { e -> logger.w(e) { "Failed to build links for ${book.enTitle}" } }

            written += 1
            if (written % 100 == 0) logger.i { "Built $written books so far" }
        }

        // Write metadata.json (as a list)
        val metaJson = buildString {
            append("[")
            metadataEntries.forEachIndexed { i, m ->
                if (i > 0) append(",")
                append(mapToJson(m))
            }
            append("]")
        }
        Files.writeString(metadataOut, metaJson)
        logger.i { "Wrote metadata with ${metadataEntries.size} entries" }

        return otzariaRoot
    }

    private fun loadToc(sefariaRoot: Path): List<TocBook> {
        val tocPath = sefariaRoot.resolve("export_toc").resolve("table_of_contents.json")
        val root = runCatching { json.parseToJsonElement(tocPath.readText()) }.getOrNull()
        val out = mutableListOf<TocBook>()
        if (root != null) walkToc(root, emptyList(), out)
        logger.i { "Loaded ToC entries: ${out.size}" }
        return out
    }

    private fun walkToc(el: JsonElement, hePath: List<String>, out: MutableList<TocBook>) {
        when (el) {
            is JsonArray -> el.forEach { walkToc(it, hePath, out) }
            is JsonObject -> {
                val contents = el["contents"]
                if (contents != null) {
                    val cat = el["heCategory"]?.jsonPrimitive?.contentOrNull
                    val path = if (cat != null) hePath + cat else hePath
                    walkToc(contents, path, out)
                } else if (el["title"] != null) {
                    val heTitle = el["heTitle"]?.jsonPrimitive?.contentOrNull ?: ""
                    val enTitle = el["title"]?.jsonPrimitive?.contentOrNull ?: ""
                    out.add(TocBook(heTitle = heTitle, enTitle = enTitle, hePath = hePath))
                }
            }
            else -> {}
        }
    }

    private fun indexTexts(sefariaRoot: Path): Map<String, IndexedText> {
        val root = sefariaRoot.resolve("json")
        val files = Files.walk(root)
            .use { s -> s.filter { it.isRegularFile() && it.name == "merged.json" && it.parent?.name == "Hebrew" }.toList() }
        val schemasRoot = sefariaRoot.resolve("export_schemas").resolve("schemas")
        val out = mutableMapOf<String, IndexedText>()
        for (f in files) {
            // The book title is the directory right above Hebrew: .../<Book>/Hebrew/merged.json
            val enTitle = f.parent?.parent?.name ?: continue
            val normalized = normalizeTitle(enTitle)
            val schemaPath = guessSchemaPath(schemasRoot, enTitle)
            out[normalized] = IndexedText(enTitle = enTitle, textPath = f, schemaPath = schemaPath)
        }
        logger.i { "Indexed ${out.size} Hebrew texts" }
        return out
    }

    private fun guessSchemaPath(schemasRoot: Path, enTitle: String): Path? {
        // Heuristic: Sefaria exports schemas as Title with punctuation replaced by underscores.
        fun normName(s: String): String {
            return s.replace(Regex("[^A-Za-z0-9]+"), "_").trim('_')
        }
        val cand1 = schemasRoot.resolve("${normName(enTitle)}.json")
        if (cand1.exists()) return cand1
        // Some titles include comma-separated parts; try compact variant
        val cand2 = schemasRoot.resolve("${normName(enTitle.replace(",", ""))}.json")
        if (cand2.exists()) return cand2
        return null
    }

    private fun buildContent(textPath: Path, schema: JsonObject?): List<String> {
        val root = runCatching { json.parseToJsonElement(textPath.readText()) }.getOrNull()
        val rootObj = (root as? JsonObject) ?: return emptyList()
        // Prefer top-level "text"; fallback to versions[0].text or versions[0] if it's already an array
        val text: JsonElement = when (val t = rootObj["text"]) {
            null -> {
                val versionsEl = rootObj["versions"]
                if (versionsEl is JsonArray) {
                    val v0 = versionsEl.firstOrNull()
                    when (v0) {
                        is JsonObject -> v0["text"] ?: return emptyList()
                        null -> return emptyList()
                        else -> v0 // already an array-of-arrays text
                    }
                } else return emptyList()
            }
            else -> t
        }

        // Determine section names and depth
        val depth = schema?.get("schema")?.jsonObject?.get("depth")?.jsonPrimitive?.content?.toIntOrNull()
        val heSectionNames = schema?.get("schema")?.jsonObject?.get("heSectionNames") as? JsonArray
        val sectionNames = heSectionNames?.mapNotNull { it.jsonPrimitive.contentOrNull }

        val lines = mutableListOf<String>()
        // Add top-level title and a first author line (from schema) like the Python does
        val heTitle = schema?.get("heTitle")?.jsonPrimitive?.contentOrNull
        heTitle?.let { addHeading(lines, 1, it) }
        val authors = ((schema?.get("authors") as? JsonArray)
            ?.mapNotNull { it.jsonObject["he"]?.jsonPrimitive?.contentOrNull }
            ?: emptyList())
        lines += ((authors.joinToString(", ") ) + "\n")

        // Recursively flatten nested arrays with headings
        recursiveSections(
            lines = lines,
            sectionNames = sectionNames,
            node = text,
            depth = depth ?: inferDepth(text),
            level = 2,
        )
        return lines
    }

    private fun inferDepth(el: JsonElement): Int {
        return when (el) {
            is JsonArray -> if (el.isEmpty()) 0 else 1 + inferDepth(el.first())
            else -> 0
        }
    }

    private fun recursiveSections(
        lines: MutableList<String>,
        sectionNames: List<String>?,
        node: JsonElement,
        depth: Int,
        level: Int,
    ) {
        val skip = setOf("שורה", "פירוש", "פסקה", "Line", "Comment", "Paragraph")
        if (depth == 0 && node !is JsonObject) {
            when (node) {
                is JsonArray -> node.forEach { recursiveSections(lines, sectionNames, it, depth, level) }
                else -> {
                    val s = node.jsonPrimitive.contentOrNull?.trim()?.replace("\n", "<br>") ?: return
                    lines += s + "\n"
                }
            }
        } else if (node is JsonArray) {
            if (depth == 1 && node.size == 1 && node.firstOrNull()?.jsonPrimitive?.isString == true) {
                recursiveSections(lines, sectionNames, node.first(), depth - 1, level)
            } else {
                for ((i, child) in node.withIndex()) {
                    if (!hasValue(child)) continue
                    val label = if (!sectionNames.isNullOrEmpty()) {
                        val name = sectionNames[max(0, sectionNames.size - depth)]
                        val letter = if (name == "דף" || name == "Daf") toDaf(i) else toGematria(i + 1)
                        if (depth > 1 && !skip.contains(name)) {
                            addHeading(lines, level, "$name $letter")
                            null
                        } else if (!skip.contains(name) && letter.isNotBlank()) {
                            "($letter) "
                        } else null
                    } else null
                    if (label != null) lines += label
                    recursiveSections(lines, sectionNames, child, depth - 1, level + 1)
                }
            }
        }
    }

    private fun addHeading(lines: MutableList<String>, level: Int, text: String) {
        if (text.isBlank()) return
        val lv = level.coerceAtMost(6)
        lines += "<h$lv>$text</h$lv>\n"
    }

    private fun hasValue(node: JsonElement): Boolean {
        if (node is JsonArray) return node.any { hasValue(it) }
        val s = node.jsonPrimitive.contentOrNull
        return s != null && s.isNotBlank()
    }

    // Create footnote-like daf label as in Python to_daf
    private fun toDaf(iZeroBased: Int): String {
        val i = iZeroBased + 1
        return if (i % 2 == 0) "${toGematria(i / 2)}." else "${toGematria(i / 2)}:"
    }

    // Basic Hebrew gematria for 1.. (no geresh/gershayim formatting)
    private fun toGematria(n: Int): String {
        var num = n
        val ones = arrayOf("", "א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט")
        val tens = arrayOf("", "י", "כ", "ל", "מ", "נ", "ס", "ע", "פ", "צ")
        val hundreds = arrayOf("", "ק", "ר", "ש", "ת")
        val sb = StringBuilder()
        var x = num
        if (x >= 100) {
            val h = (x / 100).coerceAtMost(4)
            sb.append(hundreds[h])
            x %= 100
        }
        if (x >= 10) {
            val t = x / 10
            sb.append(tens[t])
            x %= 10
        }
        if (x > 0) sb.append(ones[x])
        return sb.toString()
    }

    private fun normalizeTitle(s: String): String {
        return s.lowercase().replace(Regex("[^a-z0-9]+"), " ").trim().replace(Regex("\\s+"), " ")
    }

    private fun sanitizeFilename(s: String): String {
        // Remove Hebrew diacritics; strip forbidden FS chars; normalize quotes/underscores
        return s
            .replace(Regex("[\u0591-\u05C7]"), "")
            .replace(Regex("[\\/:*\"״?<>|]"), "")
            .replace("_", " ")
            .replace("''", "")
            .replace("'", "")
            .trim()
    }

    private fun exportLinksForBook(
        linksRoot: Path,
        linksOut: Path,
        enTitle: String,
        heTitle: String,
        enToHe: Map<String, String>,
    ) {
        // links*.csv columns include: Citation 1, Citation 2, Conection Type, Text 1, Text 2, Category 1, Category 2
        val csvFiles = if (linksRoot.exists()) Files.list(linksRoot).use { it.filter { p -> p.isRegularFile() && p.name.endsWith(".csv") }.toList() } else emptyList()
        if (csvFiles.isEmpty()) return
        val norm = normalizeTitle(enTitle)
        val entries = mutableListOf<String>()
        for (csv in csvFiles) {
            val lines = runCatching { csv.readText() }.getOrDefault("").lines()
            if (lines.isEmpty()) continue
            val header = lines.first().split(',')
            val idxText1 = header.indexOfFirst { it.trim().equals("Text 1", ignoreCase = true) }
            val idxText2 = header.indexOfFirst { it.trim().equals("Text 2", ignoreCase = true) }
            val idxConn = header.indexOfFirst { it.contains("Conection", ignoreCase = true) }
            val idxCit1 = header.indexOfFirst { it.trim().startsWith("Citation 1") }
            val idxCit2 = header.indexOfFirst { it.trim().startsWith("Citation 2") }
            if (idxText1 < 0 || idxText2 < 0 || idxConn < 0 || idxCit1 < 0 || idxCit2 < 0) continue
            for (row in lines.drop(1)) {
                if (row.isBlank()) continue
                val cols = splitCsvRow(row)
                if (cols.size <= max(idxText1, idxText2)) continue
                val t1 = cols[idxText1].trim('"')
                val t2 = cols[idxText2].trim('"')
                val who = when {
                    normalizeTitle(t1) == norm -> 1
                    normalizeTitle(t2) == norm -> 2
                    else -> 0
                }
                if (who == 0) continue
                val cit1 = cols.getOrNull(idxCit1)?.trim('"') ?: continue
                val cit2 = cols.getOrNull(idxCit2)?.trim('"') ?: continue
                val conn = cols.getOrNull(idxConn)?.trim('"') ?: "other"
                // We don’t have exact line indices here; make best-effort: skip unless simple digit
                val lineIdx1 = extractLineIndex(cit1) ?: continue
                val lineIdx2 = extractLineIndex(cit2) ?: continue
                val path2TitleEn = if (who == 1) t2 else t1
                val heTarget = enToHe[normalizeTitle(path2TitleEn)] ?: path2TitleEn
                val item = mapOf(
                    "line_index_1" to lineIdx1,
                    "heRef_2" to heTarget,
                    // DatabaseGenerator extracts the target title from file name; ensure Hebrew title + .txt
                    "path_2" to "$heTarget.txt",
                    "line_index_2" to lineIdx2,
                    "Conection Type" to conn
                )
                entries += mapToJson(item)
            }
        }
        if (entries.isNotEmpty()) {
            val out = linksOut.resolve("${sanitizeFilename(heTitle)}_links.json")
            Files.writeString(out, "[" + entries.joinToString(",") + "]")
        }
    }

    private fun splitCsvRow(row: String): List<String> {
        val out = mutableListOf<String>()
        val sb = StringBuilder()
        var inQuotes = false
        var i = 0
        while (i < row.length) {
            val c = row[i]
            when (c) {
                '"' -> {
                    inQuotes = !inQuotes
                }
                ',' -> {
                    if (inQuotes) sb.append(c) else { out += sb.toString(); sb.setLength(0) }
                }
                else -> sb.append(c)
            }
            i++
        }
        out += sb.toString()
        return out
    }

    private fun extractLineIndex(citation: String): Int? {
        // Very rough: pull last number token in citation (e.g. "Book 12:3" -> 3)
        val m = Regex("(\\d+)([^0-9]*)$").find(citation.trim()) ?: return null
        return m.groupValues[1].toIntOrNull()
    }

    private fun mapToJson(map: Map<*, *>): String {
        return buildString {
            append("{")
            var first = true
            for ((k, v) in map) {
                if (!first) append(",") else first = false
                append('"').append(escapeJson(k.toString())).append('"').append(":")
                append(valueToJson(v))
            }
            append("}")
        }
    }

    private fun valueToJson(v: Any?): String = when (v) {
        null -> "null"
        is Number, is Boolean -> v.toString()
        is Iterable<*> -> v.joinToString(prefix = "[", postfix = "]") { valueToJson(it) }
        is Array<*> -> v.joinToString(prefix = "[", postfix = "]") { valueToJson(it) }
        is Map<*, *> -> mapToJson(v)
        else -> '"' + escapeJson(v.toString()) + '"'
    }

    private fun escapeJson(s: String): String = s
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")

    private fun asString(el: JsonElement?): String? = when (el) {
        null -> null
        is JsonPrimitive -> el.contentOrNull
        is JsonArray -> asString(el.firstOrNull())
        is JsonObject -> null
        else -> null
    }
}
