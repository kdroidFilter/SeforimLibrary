package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.common.ids.IdAllocatorBindings
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import io.github.kdroidfilter.seforimlibrary.core.models.PubDate
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.exists
import kotlin.io.path.name
import kotlin.io.path.readText

internal class SefariaBookPayloadReader(
    private val json: Json,
    private val logger: Logger,
    // Sefaria's wider author-name vocabulary. Defaults to empty so an export
    // without authors.json — and every existing caller — behaves as before.
    private val authorTitles: SefariaAuthorTitles = SefariaAuthorTitles.EMPTY,
    // מיפוי מוצהר של ספרים שמיובאים מקובץ גרסה ולא מ-merged.json.
    // ריק כברירת מחדל, כך שכל ספר שאינו במיפוי נקרא בדיוק כמו קודם.
    private val preferredVersions: SefariaPreferredVersions = SefariaPreferredVersions.Empty,
) {
    internal data class SelectedBookReadStats(
        val mergedFilesScanned: Int,
        val payloadsLoaded: Int,
    )

    fun buildSchemaLookup(schemaDir: Path): Map<String, Path> {
        val lookup = ConcurrentHashMap<String, Path>()
        Files.newDirectoryStream(schemaDir) { it.fileName.toString().endsWith(".json") }.use { ds ->
            for (schemaPath in ds) {
                runCatching {
                    val obj = json.parseToJsonElement(schemaPath.readText()).jsonObject
                    val schemaObj = obj["schema"]?.jsonObject ?: return@runCatching
                    listOf(schemaObj["title"]?.stringOrNull(), schemaObj["heTitle"]?.stringOrNull()).forEach { key ->
                        normalizeTitleKey(key)?.let { normalized ->
                            lookup.putIfAbsent(normalized, schemaPath)
                        }
                    }
                }
            }
        }
        return lookup
    }

    /**
     * Read and parse book files in parallel using coroutines.
     */
    suspend fun readBooksInParallel(
        jsonDir: Path,
        schemaDir: Path,
        schemaLookup: Map<String, Path>
    ): List<BookPayload> = coroutineScope {
        val mergedFiles = Files.walk(jsonDir).use { stream ->
            stream.filter { Files.isRegularFile(it) && it.fileName.name.equals("merged.json", ignoreCase = true) }
                .toList()
        }

        logger.i { "Found ${mergedFiles.size} merged.json files to process" }

        // Process files in parallel with limited concurrency
        val semaphore = Semaphore(SefariaImportTuning.FILE_PARALLELISM)

        mergedFiles.map { textPath ->
            async(Dispatchers.IO) {
                semaphore.withPermit {
                    // Derive the text-only per-line values here, on the worker that
                    // already holds the book's text, so the single-threaded insert
                    // loop is left with nothing but id allocation and inserts.
                    parseBookFile(textPath, schemaDir, schemaLookup)?.precomputeLineData()
                }
            }
        }.awaitAll().filterNotNull()
    }

    /**
     * Streams only top-level titles for all merged files and materializes
     * candidate payloads one at a time. Payload construction remains identical
     * to the regular importer, while unrelated book text never enters memory.
     */
    internal fun readSelectedBooks(
        jsonDir: Path,
        schemaDir: Path,
        schemaLookup: Map<String, Path>,
        candidatePrimaryHeTitles: Set<String>,
        consume: (BookPayload) -> Unit,
    ): SelectedBookReadStats {
        var scanned = 0
        var loaded = 0
        val mergedFiles = Files.walk(jsonDir).use { stream ->
            stream.filter { Files.isRegularFile(it) && it.fileName.name.equals("merged.json", ignoreCase = true) }
                .sorted()
                .toList()
        }
        for (textPath in mergedFiles) {
            scanned++
            val header = readTopLevelTitles(textPath)
            val schemaPath = resolveSchemaPath(
                title = header.first,
                heTitle = header.second,
                folderName = textPath.parent?.fileName?.name,
                schemaDir = schemaDir,
                lookup = schemaLookup,
            ) ?: continue
            val schemaHeader = readSchemaTitles(schemaPath)
            if (sequenceOf(header.second, schemaHeader.second).filterNotNull()
                    .none { it in candidatePrimaryHeTitles }
            ) continue
            parseBookFile(textPath, schemaDir, schemaLookup)?.let {
                loaded++
                consume(it)
            }
        }
        return SelectedBookReadStats(scanned, loaded)
    }

    private fun readTopLevelTitles(path: Path): Pair<String?, String?> {
        var title: String? = null
        var heTitle: String? = null
        JsonFactory().createParser(path.toFile()).use { parser ->
            require(parser.nextToken() == JsonToken.START_OBJECT) { "Expected object in $path" }
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                require(parser.currentToken == JsonToken.FIELD_NAME) { "Expected field in $path" }
                val name = parser.currentName
                val token = parser.nextToken()
                if (token == JsonToken.VALUE_STRING && name == "title") title = parser.text
                if (token == JsonToken.VALUE_STRING && name == "heTitle") heTitle = parser.text
                parser.skipChildren()
            }
        }
        return title to heTitle
    }

    private fun readSchemaTitles(path: Path): Pair<String?, String?> {
        val root = json.parseToJsonElement(path.readText()).jsonObject
        val schema = root["schema"]?.jsonObject ?: return null to null
        return schema["title"]?.stringOrNull() to schema["heTitle"]?.stringOrNull()
    }

    internal fun parseBookFile(
        textPath: Path,
        schemaDir: Path,
        schemaLookup: Map<String, Path>
    ): BookPayload? {
        return runCatching {
            val mergedJson = json.parseToJsonElement(textPath.readText()).jsonObject
            val preferredPath = preferredVersions.resolve(
                mergedPath = textPath,
                enTitle = mergedJson["title"]?.stringOrNull(),
                heTitle = mergedJson["heTitle"]?.stringOrNull(),
            )
            val textJson = preferredPath
                ?.let { json.parseToJsonElement(it.readText()).jsonObject }
                ?: mergedJson
            val fileTitle = textJson["title"]?.stringOrNull()
            val fileHeTitle = textJson["heTitle"]?.stringOrNull()
            val folderName = textPath.parent?.fileName?.name

            val schemaPath = resolveSchemaPath(
                title = fileTitle,
                heTitle = fileHeTitle,
                folderName = folderName,
                schemaDir = schemaDir,
                lookup = schemaLookup
            ) ?: return@runCatching null

            val schemaJson = json.parseToJsonElement(schemaPath.readText()).jsonObject
            val schemaObj = schemaJson["schema"]?.jsonObject ?: return@runCatching null
            val englishTitle = schemaObj["title"]?.stringOrNull() ?: fileTitle ?: folderName ?: return@runCatching null
            val hebrewTitle = schemaObj["heTitle"]?.stringOrNull() ?: fileHeTitle ?: englishTitle

            val textElement = textJson["text"] ?: return@runCatching null
            val categories = schemaJson["heCategories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: schemaObj["heCategories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: textJson["categories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: emptyList()

            // Unsanitized: the whole-unit ref sets gate on Sefaria's own
            // English category paths ("Talmud/Bavli", "Mishnah", …).
            val categoriesEn = schemaJson["categories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: schemaObj["categories"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull }
                ?: emptyList()

            // `slug` is the topic id authors.json is keyed by; it is what lets a
            // bare "אברהם יצחק הכהן קוק" become "הרב אברהם יצחק הכהן קוק".
            // Without authors.json this resolves to the schema name unchanged.
            val authorEntries = schemaJson["authors"]?.jsonArray?.mapNotNull { author ->
                resolveSefariaAuthorName(author)?.let { he ->
                    he to author.jsonObject["slug"]?.stringOrNull()
                }
            }.orEmpty()
            val authors = authorEntries.map { (he, slug) -> authorTitles.displayName(slug, he) }
            // Keep every form for blacklist matching — see BookPayload.authorMatchKeys.
            val authorMatchKeys = authorEntries.flatMap { (he, slug) ->
                listOf(he) + authorTitles.allNameForms(slug)
            }.distinct()

            val (lines, refs, headings, cleanShifts, lineKeyHashOverrides, simanTopicLines) = buildBookContent(
                schemaObj = schemaObj,
                textElement = textElement,
                bookHeTitle = hebrewTitle,
                bookEnTitle = englishTitle,
                authors = authors
            )
            // merged.json "versions" is a list of [title, url] pairs. A single
            // entry means the merged text IS that version verbatim — the
            // precondition for exact charLevelData offsets.
            val singleVersionTitle = (textJson["versions"] as? JsonArray)
                ?.takeIf { it.size == 1 }
                ?.let { (it.first() as? JsonArray)?.firstOrNull()?.stringOrNull() }
                ?.trim()?.takeIf { it.isNotEmpty() }
            val versionsMeta = (textJson["versions"] as? JsonArray)?.mapNotNull { el ->
                val pair = el as? JsonArray ?: return@mapNotNull null
                val title = pair.getOrNull(0)?.stringOrNull()?.trim()?.takeIf { it.isNotEmpty() }
                    ?: return@mapNotNull null
                VersionMeta(title = title, source = pair.getOrNull(1)?.stringOrNull())
            } ?: emptyList()
            val description = extractDescription(schemaJson, schemaObj)
            val heShortDesc = extractShortDescription(schemaJson, schemaObj)
            val pubDates = extractPubDates(schemaJson, schemaObj)
            val altStructures = SefariaAltNodeNameFixes.apply(englishTitle, parseAltStructures(schemaJson), logger)
            val rawDependence = extractRawDependence(schemaJson, schemaObj)
            val dependence = rawDependence?.let(::mapDependence)
            val declaredBaseTextTitleKeys = extractBaseTextTitleKeys(schemaJson, schemaObj)
            // When Sefaria didn't ship `base_text_titles` (≈ 74 books, e.g.
            // Nachalat Avot on Avot, Bartenura on Torah, Ralbag on Torah),
            // try to recover the base from the title's "X on Y" / "X על Y"
            // pattern. Kept separate from declared keys — provenance INFERRED.
            val inferredBaseTextTitleKeys = if (declaredBaseTextTitleKeys.isEmpty() && dependence != null) {
                extractBaseKeysFromTitle(englishTitle, hebrewTitle)
            } else {
                emptyList()
            }
            val collectiveTitleObj = schemaJson["collective_title"] as? JsonObject
            val collectiveTitleEn = collectiveTitleObj?.get("en")?.stringOrNull()?.trim()?.takeIf { it.isNotEmpty() }
            val collectiveTitleHe = collectiveTitleObj?.get("he")?.stringOrNull()?.trim()?.takeIf { it.isNotEmpty() }
            val titleAliasKeys = extractTitleAliasKeys(schemaJson, schemaObj)
            val isReferenceWork = schemaJson["categories"]?.jsonArray
                ?.firstOrNull()?.jsonPrimitive?.contentOrNull == "Reference"

            BookPayload(
                heTitle = hebrewTitle,
                enTitle = englishTitle,
                categoriesHe = flattenTalmudCategories(categories.map { sanitizeFolder(it) }),
                lines = lines,
                refEntries = refs,
                headings = headings,
                authors = authors,
                authorMatchKeys = authorMatchKeys,
                description = description,
                heShortDesc = heShortDesc,
                pubDates = pubDates,
                altStructures = altStructures,
                dependence = dependence,
                rawDependence = rawDependence,
                declaredBaseTextTitleKeys = declaredBaseTextTitleKeys,
                inferredBaseTextTitleKeys = inferredBaseTextTitleKeys,
                collectiveTitleHe = collectiveTitleHe,
                collectiveTitleEn = collectiveTitleEn,
                titleAliasKeys = titleAliasKeys,
                singleVersionTitle = singleVersionTitle,
                cleanShiftByLineIndex = cleanShifts,
                lineKeyHashOverrides = lineKeyHashOverrides,
                simanTopicLines = simanTopicLines,
                versionsMeta = versionsMeta,
                sourceDirPath = textPath.parent?.toString(),
                schemaFilePath = schemaPath.toString(),
                preferredVersionFileName = preferredPath?.fileName?.name,
                categoriesEn = categoriesEn,
                boldDashDibburim = SefariaTalmudDibburBold.appliesTo(categoriesEn, dependence, lines),
                isReferenceWork = isReferenceWork,
            )
        }.onFailure { e ->
            // גרסה מוצהרת שחסרה בייצוא היא שגיאת קלט — לא בולעים אותה.
            if (e is MissingPreferredVersionException) throw e
            if (e is SefariaSchemaException) throw e
            logger.w(e) { "Failed to prepare book from $textPath" }
        }.getOrNull()
    }

    private fun resolveSchemaPath(
        title: String?,
        heTitle: String?,
        folderName: String?,
        schemaDir: Path,
        lookup: Map<String, Path>
    ): Path? {
        val candidates = listOfNotNull(title, heTitle, folderName?.replace('_', ' '), folderName)
        for (candidate in candidates) {
            val normalized = normalizeTitleKey(candidate)
            if (normalized != null) {
                val fromLookup = lookup[normalized]
                if (fromLookup != null) return fromLookup
            }
            val path = schemaDir.resolve("${candidate.replace(" ", "_")}.json")
            if (path.exists()) return path
        }
        return null
    }

    /**
     * When a book has `dependence != null` but no `base_text_titles`, try to
     * recover the declared base from the title's "X on Y" or "X על Y" pattern.
     * Returns the normalized title key(s) for "Y", which the importer's first
     * pass will resolve against `normalizedTitleToBookId` (including aliases).
     *
     * Drops empty or whitespace-only results — unresolvable keys are silently
     * skipped at resolution time, so we don't filter further here.
     */
    private fun extractBaseKeysFromTitle(enTitle: String?, heTitle: String?): List<String> {
        val out = mutableListOf<String>()
        enTitle?.let {
            val idx = it.lastIndexOf(" on ")
            if (idx > 0) {
                normalizeTitleKey(it.substring(idx + 4).trim())?.let(out::add)
            }
        }
        heTitle?.let {
            val idx = it.lastIndexOf(" על ")
            if (idx > 0) {
                normalizeTitleKey(it.substring(idx + 4).trim())?.let(out::add)
                // Also try without the leading definite article "ה" (e.g. "התורה" → "תורה").
                val raw = it.substring(idx + 4).trim()
                if (raw.startsWith("ה") && raw.length > 1) {
                    normalizeTitleKey(raw.substring(1))?.let(out::add)
                }
            }
        }
        return out.distinct()
    }

    /**
     * Extracts all Sefaria-recognised title aliases for the book. Used by the
     * importer to populate `normalizedTitleToBookId` so that title-pattern
     * base parsing (e.g. "X on Avot" → Pirkei Avot) can resolve abbreviated
     * names that aren't the primary `title`/`heTitle`.
     *
     * Filters out HTML-marked variants (`<i>Pirkei Avot</i>`) to avoid noise.
     */
    private fun extractTitleAliasKeys(schemaJson: JsonObject, schemaObj: JsonObject): List<String> {
        val out = mutableListOf<String>()
        fun ingest(key: String) {
            (schemaJson[key]?.jsonArray ?: schemaObj[key]?.jsonArray)?.forEach { el ->
                val s = el.jsonPrimitive.contentOrNull?.takeIf { it.isNotBlank() } ?: return@forEach
                if (s.contains('<') || s.contains('>')) return@forEach
                normalizeTitleKey(s)?.let(out::add)
            }
        }
        ingest("titleVariants")
        ingest("heTitleVariants")
        return out.distinct()
    }

    // Raw `dependence` after trim + canonical lowercasing; null when absent.
    private fun extractRawDependence(schemaJson: JsonObject, schemaObj: JsonObject): String? =
        (schemaJson["dependence"]?.stringOrNull() ?: schemaObj["dependence"]?.stringOrNull())
            ?.trim()?.lowercase()?.takeIf { it.isNotEmpty() }

    private fun mapDependence(raw: String): Dependence = when (raw) {
        "commentary", "sub-commentary", "supercommentary", "super-commentary",
        "super_commentary" -> Dependence.COMMENTARY
        "targum" -> Dependence.TARGUM
        "midrash" -> Dependence.MIDRASH
        else -> Dependence.OTHER_DEPENDANT
    }

    private fun extractBaseTextTitleKeys(schemaJson: JsonObject, schemaObj: JsonObject): List<String> {
        val arr = schemaJson["base_text_titles"]?.jsonArray
            ?: schemaObj["base_text_titles"]?.jsonArray
            ?: return emptyList()
        val keys = mutableListOf<String>()
        arr.forEach { el ->
            when (el) {
                is JsonObject -> {
                    el["en"]?.stringOrNull()?.let { normalizeTitleKey(it)?.let(keys::add) }
                    el["he"]?.stringOrNull()?.let { normalizeTitleKey(it)?.let(keys::add) }
                }
                is JsonPrimitive -> el.contentOrNull?.let { normalizeTitleKey(it)?.let(keys::add) }
                else -> {}
            }
        }
        return keys.distinct()
    }

    private fun extractDescription(schemaJson: JsonObject, schemaObj: JsonObject): String? {
        return schemaJson["heDesc"]?.stringOrNull()
            ?: schemaObj["heDesc"]?.stringOrNull()
            ?: schemaJson["description"]?.stringOrNull()
            ?: schemaObj["description"]?.stringOrNull()
            ?: schemaJson["heDescription"]?.stringOrNull()
            ?: schemaObj["heDescription"]?.stringOrNull()
    }

    // Real one-line summary → book.heShortDesc (long text goes to heDesc via extractDescription).
    private fun extractShortDescription(schemaJson: JsonObject, schemaObj: JsonObject): String? {
        return schemaJson["heShortDesc"]?.stringOrNull()
            ?: schemaObj["heShortDesc"]?.stringOrNull()
    }

    private fun extractPubDates(schemaJson: JsonObject, schemaObj: JsonObject): List<PubDate> {
        val dates = mutableListOf<String>()
        fun collect(key: String, obj: JsonObject) {
            obj[key]?.let { el ->
                when (el) {
                    is JsonArray -> dates += el.mapNotNull { it.jsonPrimitive.contentOrNull }
                    is JsonPrimitive -> el.contentOrNull?.let { dates += it }
                    else -> {}
                }
            }
        }
        collect("pubDate", schemaJson)
        collect("pubDate", schemaObj)
        return dates.distinct().map { PubDate(date = it) }
    }

    private fun parseAltStructures(schemaJson: JsonObject): List<AltStructurePayload> {
        val altsObj = schemaJson["alts"]?.jsonObject
            ?: schemaJson["alt_structs"]?.jsonObject
            ?: return emptyList()
        return altsObj.mapNotNull { (key, value) ->
            val altObj = value.jsonObject
            val nodesArray = altObj["nodes"]?.jsonArray ?: return@mapNotNull null
            val nodes = nodesArray.mapNotNull { parseAltNode(it.jsonObject) }
            AltStructurePayload(
                key = key,
                title = altObj["title"]?.stringOrNull(),
                heTitle = altObj["heTitle"]?.stringOrNull(),
                nodes = nodes
            )
        }
    }

    private fun parseAltNode(obj: JsonObject): AltNodePayload {
        val title = obj["title"]?.stringOrNull()
        val heTitle = obj["heTitle"]?.stringOrNull()
        val wholeRef = obj["wholeRef"]?.stringOrNull()
        val refs = obj["refs"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
        val addressTypes = obj["addressTypes"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
        val addresses = obj["addresses"]?.jsonArray?.mapNotNull { it.jsonPrimitive.intOrNull } ?: emptyList()
        val skippedAddresses = obj["skipped_addresses"]?.jsonArray?.mapNotNull { it.jsonPrimitive.intOrNull } ?: emptyList()
        val startingAddress = obj["startingAddress"]?.stringOrNull()
        val offset = obj["offset"]?.jsonPrimitive?.intOrNullSafe()
        val childLabel = obj["heSectionNames"]?.jsonArray?.firstOrNull()?.jsonPrimitive?.contentOrNull
            ?: obj["sectionNames"]?.jsonArray?.firstOrNull()?.jsonPrimitive?.contentOrNull
        val children = obj["nodes"]?.jsonArray?.mapNotNull { parseAltNode(it.jsonObject) } ?: emptyList()
        // Sefaria's `has_scope_alone_match_template()`; a scope-less template
        // defaults to "combined" and does not make the node citable alone.
        val referenceableAlone = obj["match_templates"]?.jsonArray?.any { template ->
            template.jsonObject["scope"]?.stringOrNull() in ALONE_MATCH_TEMPLATE_SCOPES
        } ?: false
        return AltNodePayload(
            title = title,
            heTitle = heTitle,
            wholeRef = wholeRef,
            refs = refs,
            addressTypes = addressTypes,
            childLabel = childLabel,
            addresses = addresses,
            skippedAddresses = skippedAddresses,
            startingAddress = startingAddress,
            offset = offset,
            children = children,
            referenceableAlone = referenceableAlone
        )
    }

    internal data class BuiltBookContent(
        val lines: List<String>,
        val refs: List<RefEntry>,
        val headings: List<Heading>,
        val cleanShifts: Map<Int, Int>,
        val lineKeyHashOverrides: Map<Int, ByteArray>,
        val simanTopicLines: Set<Int>,
    )

    /**
     * Walks an alternative version's text with the BOOK's schema — the exact
     * walk merged.json went through, so identical addresses yield identical
     * refs and identical line formatting. Join version→merged lines by ref.
     */
    internal fun walkTextWithSchema(
        schemaObj: JsonObject,
        textElement: JsonElement,
        bookHeTitle: String,
        bookEnTitle: String,
        collectLineKeyOverrides: Boolean = true,
    ): BuiltBookContent = buildBookContent(
        schemaObj = schemaObj,
        textElement = textElement,
        bookHeTitle = bookHeTitle,
        bookEnTitle = bookEnTitle,
        authors = emptyList(),
        collectLineKeyOverrides = collectLineKeyOverrides,
    )

    private fun buildBookContent(
        schemaObj: JsonObject,
        textElement: JsonElement,
        bookHeTitle: String,
        bookEnTitle: String,
        authors: List<String>,
        collectLineKeyOverrides: Boolean = true,
    ): BuiltBookContent {
        fun walk(labels: OwnLabelPass) =
            walkBook(schemaObj, textElement, bookHeTitle, bookEnTitle, authors, collectLineKeyOverrides, labels)
        val first = OwnLabelPass()
        val built = walk(first)
        // A section that labels most of its own items also writes some numbers its own way:
        // "טוב." for 17 and "חי." for 18 (קשר גודל), "<b>רן </b>" for 250 (the indexes of
        // תרומת הדשן). The book is walked again with those spellings accepted there, and
        // only there: anywhere else "אחד." at the thirteenth place is a dibbur, not a label.
        val variantSections = first.sectionsAdmittingVariants()
        return if (variantSections.isEmpty()) built else walk(OwnLabelPass(variantSections))
    }

    private fun walkBook(
        schemaObj: JsonObject,
        textElement: JsonElement,
        bookHeTitle: String,
        bookEnTitle: String,
        authors: List<String>,
        collectLineKeyOverrides: Boolean,
        labels: OwnLabelPass,
    ): BuiltBookContent {
        // Pre-allocate with estimated capacity
        val output = ArrayList<String>(1000)
        val refs = ArrayList<RefEntry>(1000)
        val headings = ArrayList<Heading>(100)
        // See BookPayload.cleanShiftByLineIndex — sparse raw-offset bookkeeping.
        val cleanShifts = HashMap<Int, Int>()
        val lineKeyHashOverrides = if (collectLineKeyOverrides) HashMap<Int, ByteArray>() else null
        val simanTopicLines = HashSet<Int>()

        fun headingTagForLevel(level: Int): Pair<String, String> = when (level) {
            0 -> "<h1>" to "</h1>"
            1 -> "<h2>" to "</h2>"
            2 -> "<h3>" to "</h3>"
            3 -> "<h4>" to "</h4>"
            else -> "<h5>" to "</h5>"
        }

        val tag = headingTagForLevel(0)
        output += "${tag.first}$bookHeTitle${tag.second}"
        authors.forEach { output += it }
        headings += Heading(title = bookHeTitle, level = 0, lineIndex = 0)

        fun processNode(
            node: JsonObject,
            text: JsonElement?,
            level: Int,
            refPrefix: String,
            heRefPrefix: String
        ) {
            val nodeTitle = node["heTitle"]?.stringOrNull()?.takeIf { it.isNotBlank() }
                ?: node["title"]?.stringOrNull()?.takeIf { it.isNotBlank() }
            val hasTitle = nodeTitle != null
            if (hasTitle && level > 0) {
                val tag = headingTagForLevel(level)
                output += "${tag.first}$nodeTitle${tag.second}"
                headings += Heading(
                    title = nodeTitle,
                    level = level,
                    lineIndex = output.size - 1
                )
            }

            if (text !is JsonArray && text !is JsonPrimitive && text !is JsonObject) return

            if (node.containsKey("nodes")) {
                val children = node["nodes"]?.jsonArray ?: JsonArray(emptyList())
                for (child in children) {
                    val childNode = child.jsonObject
                    val childText = selectNodeText(childNode, text)
                    val childTitle = childNode["title"]?.stringOrNull().orEmpty()
                    val childHeTitle = childNode["heTitle"]?.stringOrNull().orEmpty()
                    val key = childNode["key"]?.stringOrNull()
                    val nextRefPrefix = buildString {
                        append(refPrefix)
                        if (!key.equals("default", ignoreCase = true) && childTitle.isNotBlank()) append(childTitle).append(", ")
                    }
                    val nextHeRefPrefix = buildString {
                        append(heRefPrefix)
                        if (!key.equals("default", ignoreCase = true) && childHeTitle.isNotBlank()) append(childHeTitle).append(", ")
                    }
                    processNode(childNode, childText, level + 1, nextRefPrefix, nextHeRefPrefix)
                }
            } else {
                val sectionNames = readHeSectionNames(node)
                val depth = node["depth"]?.jsonPrimitive?.intOrNullSafe() ?: sectionNames.size
                val addressTypes = node["addressTypes"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
                val referenceableSections = node["referenceableSections"]?.jsonArray?.mapNotNull { it.jsonPrimitive.booleanOrNull } ?: emptyList()
                val indexOffsets = readIndexOffsets(node, depth)
                val nextLevel = if (hasTitle) level + 1 else level
                recursiveSections(
                    sectionNames = sectionNames,
                    text = text,
                    depth = depth,
                    level = nextLevel,
                    output = output,
                    refEntries = refs,
                    refPrefix = "$refPrefix ",
                    heRefPrefix = "$heRefPrefix ",
                    bookEnTitle = bookEnTitle,
                    bookHeTitle = bookHeTitle,
                    headings = headings,
                    addressTypes = addressTypes,
                    referenceableSections = referenceableSections,
                    refIndexOffset = indexOffsets?.top ?: 0,
                    childRefOffsets = indexOffsets?.children,
                    cleanShifts = cleanShifts,
                    lineKeyHashOverrides = lineKeyHashOverrides,
                    simanTopicLines = simanTopicLines,
                    numbersIntegerLeaf = numbersIntegerLeaf(node),
                    labels = labels.section(refPrefix),
                )
            }
        }

        if (schemaObj.containsKey("nodes")) {
            val nodes = schemaObj["nodes"]?.jsonArray ?: JsonArray(emptyList())
            for (nodeElement in nodes) {
                val node = nodeElement.jsonObject
                val nodeText = selectNodeText(node, textElement)
                val nodeTitle = node["title"]?.stringOrNull().orEmpty()
                val nodeHeTitle = node["heTitle"]?.stringOrNull().orEmpty()
                val key = node["key"]?.stringOrNull()
                val refPrefix = buildString {
                    append(bookEnTitle)
                    append(", ")
                    if (!key.equals("default", ignoreCase = true) && nodeTitle.isNotBlank()) append(nodeTitle).append(", ")
                }
                val heRefPrefix = buildString {
                    append(bookHeTitle)
                    append(", ")
                    if (!key.equals("default", ignoreCase = true) && nodeHeTitle.isNotBlank()) append(nodeHeTitle).append(", ")
                }
                processNode(node, nodeText, 1, refPrefix, heRefPrefix)
            }
        } else {
            val sectionNames = readHeSectionNames(schemaObj)
            val depth = schemaObj["depth"]?.jsonPrimitive?.intOrNullSafe() ?: sectionNames.size
            val addressTypes = schemaObj["addressTypes"]?.jsonArray?.mapNotNull { it.jsonPrimitive.contentOrNull } ?: emptyList()
            val referenceableSections = schemaObj["referenceableSections"]?.jsonArray?.mapNotNull { it.jsonPrimitive.booleanOrNull } ?: emptyList()
            val indexOffsets = readIndexOffsets(schemaObj, depth)
            recursiveSections(
                sectionNames = sectionNames,
                text = textElement,
                depth = depth,
                level = 1,
                output = output,
                refEntries = refs,
                refPrefix = "$bookEnTitle ",
                // Comma after the title, like the named-nodes path above: consumers
                // count ", "-separated address levels, and a space-glued title made
                // the first level uncountable (e.g. "רש"י על חולין ו., ה, ב").
                heRefPrefix = "$bookHeTitle, ",
                bookEnTitle = bookEnTitle,
                bookHeTitle = bookHeTitle,
                headings = headings,
                addressTypes = addressTypes,
                referenceableSections = referenceableSections,
                refIndexOffset = indexOffsets?.top ?: 0,
                childRefOffsets = indexOffsets?.children,
                cleanShifts = cleanShifts,
                lineKeyHashOverrides = lineKeyHashOverrides,
                simanTopicLines = simanTopicLines,
                numbersIntegerLeaf = numbersIntegerLeaf(schemaObj),
                labels = labels.section(bookEnTitle),
            )
        }

        return BuiltBookContent(output, refs, headings, cleanShifts, lineKeyHashOverrides.orEmpty(), simanTopicLines)
    }

    private fun recursiveSections(
        sectionNames: List<String?>,
        text: JsonElement?,
        depth: Int,
        level: Int,
        output: MutableList<String>,
        refEntries: MutableList<RefEntry>,
        refPrefix: String,
        heRefPrefix: String,
        bookEnTitle: String,
        bookHeTitle: String,
        headings: MutableList<Heading>,
        linePrefix: String = "",
        addressTypes: List<String> = emptyList(),
        referenceableSections: List<Boolean> = emptyList(),
        // Offsets applied to the *English* ref number built at this iteration.
        // Propagated from `index_offsets_by_depth` so Zohar-style books emit
        // the same global paragraph indices that Sefaria links use (e.g.
        // "Zohar, Tetzaveh 14:127" instead of "14:13"). See readIndexOffsets.
        refIndexOffset: Int = 0,
        // Offsets to hand down to the *children* of the iteration at this
        // level (one entry per outer-dim index).
        childRefOffsets: List<Int>? = null,
        // Sparse raw-offset bookkeeping (see BookPayload.cleanShiftByLineIndex).
        cleanShifts: MutableMap<Int, Int>? = null,
        lineKeyHashOverrides: MutableMap<Int, ByteArray>? = null,
        simanTopicLines: MutableSet<Int>? = null,
        numbersIntegerLeaf: Boolean = false,
        labels: SectionLabels,
    ) {
        // Leaf when depth reached zero, OR when the data is shallower than the
        // schema declares (e.g. Keter Malkhut: schema says depth=2 but most
        // chapters are stored as a single string instead of a paragraph array).
        // Falling through to `text !is JsonArray return` would silently drop
        // the chapter's content.
        val leafPrimitive = text as? JsonPrimitive
        if (depth == 0 || (leafPrimitive != null && leafPrimitive.isString)) {
            val content = leafPrimitive?.takeIf { it.isString }?.content
            if (!content.isNullOrEmpty()) {
                val collapseBreaks = collapsesInlineBreaks(bookEnTitle)
                val stripFootnotes = hasTranslatorFootnotes(refPrefix)
                // Otzaria: Mishnah Berurah's "{א}" markers stay as Sefaria prints them. Line ids
                // are content-keyed here, so normalizing them would re-key 1,011 lines and move
                // the Shaar HaTziyun anchors stored in otzaria-library.
                fun clean(collapse: Boolean) =
                    cleanSefariaLine(content, collapseInlineBreaks = collapse, stripFootnotes = stripFootnotes)
                val normalized = clean(collapseBreaks)
                // A second walk of the book repeats the same separations: count them once.
                val cleaned = SefariaDashlessDibburim.separate(bookHeTitle, normalized, recordStats = !labels.repeatedWalk)
                if (cleaned.isNotEmpty()) {
                    val topic = if (output.lastOrNull()?.startsWith("<h") == true) {
                        SefariaSimanTopicLines.split(bookHeTitle, cleaned)
                    } else {
                        null
                    }
                    if (topic != null) {
                        simanTopicLines?.add(output.size)
                        output += topic.line
                    }
                    val stored = topic?.seif ?: cleaned
                    // Reproduce the old pipeline BEFORE the dashless repair: keeping
                    // a break can either enable or suppress that repair. Collapsing
                    // the final rendered line cannot undo an already inserted dash.
                    // Store only changed hashes, so plain lines need no extra scan
                    // during precomputation and we retain no second copy of the text.
                    if (lineKeyHashOverrides != null) {
                        val keyContent = if (!collapseBreaks && "<br>" in normalized) {
                            SefariaDashlessDibburim.separate(
                                bookHeTitle,
                                clean(collapse = true),
                                recordStats = false,
                            )
                        } else {
                            cleaned
                        }
                        if (keyContent != stored) {
                            lineKeyHashOverrides[output.size] = IdAllocatorBindings.lineNaturalKeyHash(keyContent)
                        }
                    }
                    output += linePrefix + stored
                    if (cleanShifts != null) {
                        if (stored != content) {
                            // Keep the char-offset gate and the generated-prefix
                            // length in one value: anchors reject every negative
                            // value, while the line-key path can still strip the
                            // prefix from cleaned content.
                            cleanShifts[output.size - 1] = cleanedLineShift(linePrefix.length)
                        } else if (linePrefix.isNotEmpty()) {
                            cleanShifts[output.size - 1] = linePrefix.length
                        }
                    }
                    val cleanRef = trimTrailingSeparators(refPrefix)
                    val cleanHeRef = trimTrailingSeparators(heRefPrefix)
                    refEntries.add(
                        RefEntry(
                            ref = cleanRef,
                            heRef = cleanHeRef,
                            path = "",
                            lineIndex = output.size
                        )
                    )
                }
            }
            return
        }
        if (text !is JsonArray) return
        val index = (sectionNames.size - depth).coerceAtLeast(0)
        // null marks an unnamed level (Sefaria "Integer"): its heading is the bare number.
        val sectionName = sectionNames.getOrElse(index) { "" }

        val nonEmptyCount = if (depth == 1) {
            text.count { !it.isTriviallyEmpty() }
        } else {
            0
        }

        val prefixesLeaf = addressTypes.getOrNull(addressTypes.size - depth) != "Integer" || numbersIntegerLeaf

        // Prefixing a self-numbered array would double its printed markers.
        // Guarded by the same conditions as nextLinePrefix so no wasted scan.
        val sourceNumbered = depth == 1 && nonEmptyCount > 1 &&
            (referenceableSections.getOrNull(sectionNames.size - depth) ?: true) &&
            prefixesLeaf &&
            when (sourceMarkerRun(text)) {
                MarkerRun.CONSECUTIVE -> true
                MarkerRun.BROKEN -> {
                    logger.d { "Partially numbered leaf array in '$bookHeTitle' ($refPrefix); keeping generated prefixes except on self-labelled items" }
                    false
                }
                MarkerRun.NONE -> false
            }

        // Items that drop the generated prefix because they already carry their own
        // label for that same number. Most shapes are enough alone. Two can also be
        // something else: a numeral that is all of a bold dibbur ("<b>י"א. </b>" is
        // "יש אומרים", "<b>  לא  </b> פלוג") and a label after an opening heading
        // ("<br>א." opens a list of topics). Those count only when another item of
        // the array carries its own label too.
        val ownLabelled: BooleanArray? = if (depth == 1 && nonEmptyCount > 1 && prefixesLeaf && !sourceNumbered) {
            val found = text.mapIndexed { idx, item -> ownLabel(item, idx + 1, labels.variants) }
            val corroborated = found.count { it != null } >= 2
            BooleanArray(text.size) { idx -> found[idx]?.let { it == LabelKind.PLAIN || corroborated } == true }
        } else {
            null
        }

        // גרסה חלקית מותרת (מערך קצר); פרק בלי הסטה — לא.
        if (childRefOffsets != null && childRefOffsets.size < text.size) {
            throw SefariaSchemaException(
                "index_offsets_by_depth has ${childRefOffsets.size} entries for ${text.size} sections at '$refPrefix'"
            )
        }

        text.forEachIndexed { idx, item ->
            if (item.isTriviallyEmpty()) return@forEachIndexed

            val currentAddressType = addressTypes.getOrNull(addressTypes.size - depth)
            val letter = when (currentAddressType) {
                "Talmud" -> toDaf(idx + 1)
                "Integer" -> toGematria(idx + 1)
                else -> toGematria(idx + 1)
            }

            val sectionIndex = sectionNames.size - depth
            val isReferenceable = referenceableSections.getOrNull(sectionIndex) ?: true
            // An item that already carries its own label for this very number keeps
            // that label alone: "(נח) [נח]" is noise. Any other number keeps the prefix.
            val numbered = depth == 1 && isReferenceable && prefixesLeaf && nonEmptyCount > 1 && !sourceNumbered &&
                !hasSelfLabeledSegments(bookEnTitle)
            val selfLabelled = numbered && currentAddressType != "Talmud" && ownLabelled?.get(idx) == true
            if (numbered) labels.count(selfLabelled)
            val nextLinePrefix = if (numbered && !selfLabelled) "($letter) " else ""

            if (depth > 1 && sectionName?.isBlank() != true && isReferenceable) {
                val tag = when (level) {
                    1 -> "<h2>" to "</h2>"
                    2 -> "<h3>" to "</h3>"
                    3 -> "<h4>" to "</h4>"
                    else -> "<h5>" to "</h5>"
                }
                val title = sectionName?.let { "$it $letter" } ?: letter
                output += "${tag.first}$title${tag.second}"
                headings += Heading(
                    title = title,
                    level = level,
                    lineIndex = output.size - 1
                )
            }

            // Paragraph/ref offset from index_offsets_by_depth (Zohar-style):
            // for the current iteration's element i, children should build refs
            // as "(i+1):(offset[i]+childIdx+1)".
            val shiftedIdx = idx + 1 + refIndexOffset
            val newRefPrefix = buildString {
                append(refPrefix)
                val refNumber = when (currentAddressType) {
                    "Talmud" -> toEnglishDaf(shiftedIdx)
                    else -> shiftedIdx.toString()
                }
                append(refNumber)
                append(":")
            }
            val newHeRefPrefix = buildString {
                append(heRefPrefix)
                append(letter)
                append(", ")
            }
            val nextRefIndexOffset = childRefOffsets?.get(idx) ?: 0

            recursiveSections(
                sectionNames = sectionNames,
                text = item,
                depth = depth - 1,
                level = level + 1,
                output = output,
                refEntries = refEntries,
                refPrefix = newRefPrefix,
                heRefPrefix = newHeRefPrefix,
                bookEnTitle = bookEnTitle,
                bookHeTitle = bookHeTitle,
                headings = headings,
                linePrefix = nextLinePrefix,
                addressTypes = addressTypes,
                referenceableSections = referenceableSections,
                refIndexOffset = nextRefIndexOffset,
                cleanShifts = cleanShifts,
                lineKeyHashOverrides = lineKeyHashOverrides,
                simanTopicLines = simanTopicLines,
                numbersIntegerLeaf = numbersIntegerLeaf,
                labels = labels,
            )
        }
    }

    // "Integer" is Sefaria's default address type, not "unnumbered": the leaf is
    // numbered unless its own Hebrew name is blank or paragraph/comment/line-like.
    private fun numbersIntegerLeaf(node: JsonObject): Boolean {
        val he = node["heSectionNames"]?.jsonArray ?: return false
        val leafIndex = maxOf(he.size, node["sectionNames"]?.jsonArray?.size ?: 0) - 1
        val leaf = he.getOrNull(leafIndex)?.jsonPrimitive?.contentOrNull?.trim().orEmpty()
        return leaf.isNotEmpty() && leaf !in UNNUMBERED_INTEGER_LEAF_NAMES
    }

    /**
     * Reads `index_offsets_by_depth` from a schema node. Sefaria keys it by the
     * address level (1-based) it shifts:
     * - `"1"`: a single integer — the node's top-level numbering starts after it
     *   (Talmud Eser HaSefirot "List of Questions on Topics" 55…, Sulam Idra Zuta 23…).
     * - `"2"`: one integer per top-level section, shifting its children
     *   (Zohar: "Tetzaveh 14:127" is the 127th paragraph of the parasha).
     * Without them the English refs disagree with every Sefaria link to the node.
     */
    private fun readIndexOffsets(node: JsonObject, schemaDepth: Int): IndexOffsets? {
        val map = node["index_offsets_by_depth"]?.jsonObject ?: return null
        val nodeTitle = node["title"]?.jsonPrimitive?.contentOrNull
        if (!setOf("1", "2").containsAll(map.keys) || (map.containsKey("2") && schemaDepth < 2)) {
            throw SefariaSchemaException("Unsupported index_offsets_by_depth keys ${map.keys} at depth $schemaDepth in '$nodeTitle'")
        }
        val top = map["1"]?.let { schemaInt(it) ?: throw SefariaSchemaException("index_offsets_by_depth[1] is not an integer in '$nodeTitle': $it") }
        val children = map["2"]?.let { el ->
            val arr = el as? JsonArray ?: throw SefariaSchemaException("index_offsets_by_depth[2] is not an array in '$nodeTitle'")
            arr.map { schemaInt(it) ?: throw SefariaSchemaException("index_offsets_by_depth[2] has a non-integer entry in '$nodeTitle': $it") }
        }
        return IndexOffsets(top ?: 0, children)
    }

    private fun schemaInt(element: JsonElement): Int? =
        (element as? JsonPrimitive)?.takeIf { !it.isString }?.intOrNull

    private data class IndexOffsets(val top: Int, val children: List<Int>?)

    /**
     * Returns the Hebrew section names for a schema node, filling in blanks
     * with a best-effort mapping from the English `sectionNames` counterpart.
     *
     * Sefaria schemas frequently ship `heSectionNames=["", "פסקה"]` where the
     * top-level Hebrew label (e.g. "תשובה", "סימן") was never filled in; the
     * English `sectionNames` still has it. Without this fallback the importer
     * skips the matching heading and responsa-style books end up with no TOC.
     * A null entry marks an unnamed level (see [mapSectionNameToHebrew]).
     */
    private fun readHeSectionNames(obj: JsonObject): List<String?> {
        val he = obj["heSectionNames"]?.jsonArray?.map { it.jsonPrimitive.contentOrNull.orEmpty() } ?: emptyList()
        val en = obj["sectionNames"]?.jsonArray?.map { it.jsonPrimitive.contentOrNull.orEmpty() } ?: emptyList()
        val size = maxOf(he.size, en.size)
        if (size == 0) return emptyList()
        return List(size) { idx ->
            val heVal = he.getOrNull(idx)?.trim().orEmpty()
            if (heVal.isNotEmpty()) {
                heVal
            } else {
                val enVal = en.getOrNull(idx)?.trim().orEmpty()
                if (enVal.isEmpty()) "" else mapSectionNameToHebrew(enVal)
            }
        }
    }

    private fun selectNodeText(node: JsonObject, text: JsonElement?): JsonElement? {
        val key = node["key"]?.stringOrNull()
        val title = node["title"]?.stringOrNull().orEmpty()
        val obj = text as? JsonObject ?: return null
        return if (!key.equals("default", ignoreCase = true) && title.isNotBlank()) {
            obj[title]
        } else {
            obj[""] ?: obj[title]
        }
    }
}

// Integer leaves Sefaria names as paragraphs, comments or lines stay unprefixed;
// "מדרש" too, since the Simanim alt-TOC already prints its letters.
private val UNNUMBERED_INTEGER_LEAF_NAMES = setOf("פסקה", "פירוש", "פרשנות", "שורה", "מדרש")

/** `match_templates[].scope` values that make an alt-struct node citable alone. */
private val ALONE_MATCH_TEMPLATE_SCOPES = setOf("any", "alone")

// Leading printed marker that makes a whole array self-numbered: "(אות) " or
// "{אות} " (unchanged since 491a4619; an intro-shifted run of these still counts).
private val SOURCE_MARKER_REGEX = Regex("""^\s*[({]([א-ת"׳״]{1,5})[)}]\s""")

// A label an item may open with, after any opening tags, for the number [LABEL_TOKEN]:
// "(אות)", "( אות )" (נהר מצרים), "{אות}", "[אות]" (אליה רבה) and "[אות X]" (אליה
// רבה קסב:ה), which may be glued to a tag or take a colon, "[ב]:"; "אות)" (ביאור על
// ספר המצוות לרס"ג; also glued, "נה)עינוי"); "אות. " / "אות: " / "אות.</b>"
// (איסור והיתר הארוך, קשר גודל, אורחות חיים להרא"ש); and a bold numeral alone,
// "<b>אות</b>", "<b>אות </b>", "<b>אות.</b>" (סידור ספרד, תרומת הדשן), which
// [BOLD_ONLY_LABEL_REGEX] tells apart. "סעיף א" / "ס"ק א" name another book's seif
// and are not labels. A generated prefix only ever yields to its own item's label,
// for the same number: a source run shifted by an intro ("(ב) א.", כף אחת) keeps
// the generated numbering.
private const val LABEL_TOKEN = """([א-ת"׳״]{1,5})"""
private val OWN_LABEL_REGEX = Regex(
    """^\s*(?:<[^>]+>\s*)*(?:(?:\(\s*(?:אות\s+)?$LABEL_TOKEN\s*\)|\{$LABEL_TOKEN\}|\[\s*(?:אות\s+)?$LABEL_TOKEN\s*\])(?=[\s<:.]|$)|""" +
        """$LABEL_TOKEN\)|$LABEL_TOKEN[.:](?=\s|</)|<b>\s*$LABEL_TOKEN\s*[.:]?\s*</b>)"""
)

// The numeral is all there is inside a bold run: the shape of a short dibbur too.
private val BOLD_ONLY_LABEL_REGEX = Regex("""^\s*(?:<[^>]+>\s*)*<b>\s*$LABEL_TOKEN\s*[.:]?\s*</b>""")

// A heading the item opens with: tag-wrapped text alone on its first line, the
// label right after the break ("<big><strong>סימן א. …</strong></big><br>א. מיד",
// קשר גודל). Text outside the tags before the break (a bold dibbur and its
// comment, ביאור הגר"א) means the label belongs to a merged later segment.
private val LEADING_HEADING_REGEX = Regex(
    """^\s*(?:<[a-z][^>]*>\s*)+[^<]*(?:</[a-z]+>\s*)+<br\s*/?>""",
    RegexOption.IGNORE_CASE,
)

private fun labelToken(regex: Regex, content: String): String? =
    regex.find(content)?.groupValues?.drop(1)?.first { it.isNotEmpty() }

private fun markerValue(token: String): Int? = gematriaToInt[token.filter { it !in "\"׳״" }]

// Another spelling of a number, accepted only in a section that labels its own items
// ([SectionLabels]): the letters in any order and final forms at their value ("רן" 250,
// "דשמ" 344, "אך" 21, "טוב" 17), or a letter spelled out ("יוד" 10). At most four
// letters, so a word is not read as a sum.
private fun variantValue(token: String): Int? {
    val letters = token.filter { it !in "\"׳״" }
    SPELLED_OUT_NUMERALS[letters]?.let { return it }
    if (letters.length > 4) return null
    return letters.sumOf { NUMERAL_LETTER_VALUES[it] ?: return null }
}

// "יוד." for 10: seven times in קשר גודל, once in ביאור על ספר המצוות לרס"ג ("יו"ד)").
private val SPELLED_OUT_NUMERALS = mapOf("יוד" to 10)

private val NUMERAL_LETTER_VALUES: Map<Char, Int> = "אבגדהוזחטיכלמנסעפצקרשתךםןףץ".toList().zip(
    listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 20, 40, 50, 80, 90),
).toMap()

// A section accepts another spelling of a label ([variantValue]) when at least this
// many of the items it numbers, and this share of them, carry a label of their own.
// In the 2026-09-30 export a section with 20 such labels or more has either at most 21%
// of its items labelled (באר הגולה חו"מ, אורחות חיים להרא"ש) or at least 68% (קול התור;
// 92% and up elsewhere). Every item that another spelling alone would match in a section
// below the gate is a dibbur: "<b>בידי. </b>" at 26, "האחד:" at 18.
private const val MIN_OWN_LABELS = 20
private const val MIN_OWN_LABEL_PERCENT = 60

/**
 * One walk of a book's text, which tallies its labels per section (a node of the
 * schema, keyed by its ref prefix). The first walk accepts only canonical labels; a
 * second one, if any, also accepts another spelling in [variantSections].
 */
private class OwnLabelPass(private val variantSections: Set<String> = emptySet()) {
    private val sections = HashMap<String, SectionLabels>()

    fun section(key: String): SectionLabels = sections.getOrPut(key) {
        SectionLabels(variants = key in variantSections, repeatedWalk = variantSections.isNotEmpty())
    }

    fun sectionsAdmittingVariants(): Set<String> = sections.filterValues { it.admitsVariants() }.keys
}

/** Of a section's items that get a generated "(N) ", how many drop it for a label of their own. */
private class SectionLabels(val variants: Boolean, val repeatedWalk: Boolean) {
    private var numbered = 0
    private var selfLabelled = 0

    fun count(selfLabelled: Boolean) {
        numbered++
        if (selfLabelled) this.selfLabelled++
    }

    fun admitsVariants(): Boolean =
        selfLabelled >= MIN_OWN_LABELS && selfLabelled * 100 >= numbered * MIN_OWN_LABEL_PERCENT
}

/** PLAIN counts alone; MAYBE_DIBBUR (a bold-only numeral, a label after a heading) needs company. */
private enum class LabelKind { PLAIN, MAYBE_DIBBUR }

/** How the item carries its own label for [number], or null when it carries none for it. */
private fun ownLabel(item: JsonElement, number: Int, variants: Boolean): LabelKind? {
    val content = (item as? JsonPrimitive)?.takeIf { it.isString }?.content ?: return null
    fun at(text: String, regex: Regex) = labelToken(regex, text)?.let { token ->
        markerValue(token) == number || variants && variantValue(token) == number
    } == true
    if (at(content, BOLD_ONLY_LABEL_REGEX)) return LabelKind.MAYBE_DIBBUR
    if (at(content, OWN_LABEL_REGEX)) return LabelKind.PLAIN
    val afterHeading = LEADING_HEADING_REGEX.find(content)?.let { content.substring(it.range.last + 1) } ?: return null
    return if (at(afterHeading, OWN_LABEL_REGEX)) LabelKind.MAYBE_DIBBUR else null
}

// Inverse of toGematria, for validating printed markers.
private val gematriaToInt: Map<String, Int> = (1..999).associateBy { toGematria(it) }

private enum class MarkerRun { NONE, CONSECUTIVE, BROKEN }

// CONSECUTIVE = contiguous block of >=2 markers rising by 1, first value <= its
// 1-based position, unmarked items only as one leading intro or a trailing tail.
private fun sourceMarkerRun(items: JsonArray): MarkerRun {
    var next = -1
    var pre = 0
    var post = 0
    var block = 0
    items.forEachIndexed { idx, item ->
        val content = (item as? JsonPrimitive)?.contentOrNull ?: return@forEachIndexed
        if (content.isBlank()) return@forEachIndexed
        val token = labelToken(SOURCE_MARKER_REGEX, content)
        if (token == null) {
            if (block == 0) pre++ else post++
            return@forEachIndexed
        }
        if (post > 0) return MarkerRun.BROKEN
        val value = markerValue(token) ?: return MarkerRun.BROKEN
        when {
            next < 0 -> if (value > idx + 1) return MarkerRun.BROKEN else next = value + 1
            value != next -> return MarkerRun.BROKEN
            else -> next++
        }
        block++
    }
    return when {
        block == 0 -> MarkerRun.NONE
        block >= 2 && pre <= 1 && (pre == 0 || post == 0) -> MarkerRun.CONSECUTIVE
        else -> MarkerRun.BROKEN
    }
}

/** מבנה סכמה שהמייבא לא יודע לפרש — נפילה רועשת במקום דילוג שקט על הספר. */
internal class SefariaSchemaException(message: String) : IllegalStateException(message)
