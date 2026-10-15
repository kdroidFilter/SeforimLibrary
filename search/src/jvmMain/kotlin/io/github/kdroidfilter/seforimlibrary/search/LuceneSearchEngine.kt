package io.github.kdroidfilter.seforimlibrary.search

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.search.index.LuceneIndex
import io.github.kdroidfilter.seforimlibrary.search.index.LuceneIndexBackend
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import java.nio.file.Path

private val logger = Logger.withTag("LuceneSearchEngine")

/**
 * The desktop's [SearchEngine]: the common [LexicalSearchEngine] over the Lucene index at [indexDir], read in process,
 * with the lexical dictionary found beside it (or at [dictionaryPath], `-DmagicDict`, `SEFORIM_MAGIC_DICT`).
 */
class LuceneSearchEngine private constructor(
    /** The Lucene index this engine reads, for the searches that share it (the semantic search's vectors). */
    val index: LuceneIndex,
    private val engine: LexicalSearchEngine,
) : SearchEngine by engine {
    private constructor(
        index: LuceneIndex,
        indexDir: Path,
        snippetProvider: SnippetProvider?,
        dictionaryPath: Path?,
    ) : this(
        index,
        LexicalSearchEngine(index, snippetProvider, lazy { loadDictionary(indexDir, dictionaryPath) }),
    )

    constructor(
        indexDir: Path,
        snippetProvider: SnippetProvider? = null,
        analyzer: Analyzer = StandardAnalyzer(),
        dictionaryPath: Path? = null,
    ) : this(LuceneIndexBackend(indexDir, analyzer = analyzer), indexDir, snippetProvider, dictionaryPath)

    suspend fun searchAllText(
        rawQuery: String,
        near: Int = 5,
        limit: Int,
        offset: Int = 0,
    ): List<LineHit> = engine.searchAllText(rawQuery, near, limit, offset)

    suspend fun searchInBook(
        rawQuery: String,
        near: Int,
        bookId: Long,
        limit: Int,
        offset: Int = 0,
    ): List<LineHit> = engine.searchInBook(rawQuery, near, bookId, limit, offset)

    suspend fun searchInCategory(
        rawQuery: String,
        near: Int,
        categoryId: Long,
        limit: Int,
        offset: Int = 0,
    ): List<LineHit> = engine.searchInCategory(rawQuery, near, categoryId, limit, offset)

    suspend fun searchInBooks(
        rawQuery: String,
        near: Int,
        bookIds: Collection<Long>,
        limit: Int,
        offset: Int = 0,
    ): List<LineHit> = engine.searchInBooks(rawQuery, near, bookIds, limit, offset)

    private companion object {
        fun loadDictionary(
            indexDir: Path,
            dictionaryPath: Path?,
        ): MagicDictionaryIndex? {
            val candidates =
                listOfNotNull(
                    dictionaryPath,
                    System.getProperty("magicDict")?.let { Path.of(it) },
                    System.getenv("SEFORIM_MAGIC_DICT")?.let { Path.of(it) },
                    indexDir.resolveSibling("lexical.db"),
                    indexDir.resolveSibling("seforim.db").resolveSibling("lexical.db"),
                    Path.of("SeforimLibrary/SeforimMagicIndexer/magicindexer/build/db/lexical.db"),
                ).distinct()
            val firstExisting = MagicDictionaryIndex.findValidDictionary(candidates.map { it.toString() })
            if (firstExisting == null) {
                logger.d {
                    "[MagicDictionary] Missing lexical.db; search will run without dictionary expansions. " +
                        "Provide -DmagicDict=/path/lexical.db or SEFORIM_MAGIC_DICT. Checked: " +
                        candidates.joinToString()
                }
                return null
            }
            logger.d { "[MagicDictionary] Loading lexical db from $firstExisting" }
            return MagicDictionaryIndex.load(HebrewTextUtils::normalizeHebrew, firstExisting).also {
                if (it == null) logger.d { "[MagicDictionary] Failed to load lexical db at $firstExisting; continuing without dictionary expansions" }
            }
        }
    }
}

/** [MagicDictionaryIndex.load] for a [Path]. */
fun MagicDictionaryIndex.Companion.load(
    norm: (String) -> String,
    candidate: Path?,
): MagicDictionaryIndex? = load(norm, candidate?.toString())

/** [MagicDictionaryIndex.findValidDictionary] for [Path]s. */
@JvmName("findValidDictionaryPath")
fun MagicDictionaryIndex.Companion.findValidDictionary(candidates: List<Path>): Path? =
    findValidDictionary(candidates.map { it.toString() })?.let { Path.of(it) }
