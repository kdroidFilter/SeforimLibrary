package io.github.kdroidfilter.seforimlibrary.sefariasqlite

import co.touchlab.kermit.Logger
import io.github.kdroidfilter.seforimlibrary.core.models.ConnectionType
import io.github.kdroidfilter.seforimlibrary.core.models.Link
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import java.nio.file.Files
import java.nio.file.Path

internal class SefariaLinksImporter(
    private val repository: SeforimRepository,
    private val logger: Logger
) {
    suspend fun processLinksInParallel(
        linksDir: Path,
        refsByCanonical: Map<String, List<RefEntry>>,
        refsByBase: Map<String, RefEntry>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        lineIdToBookId: Map<Long, Long>,
        bookMetaById: Map<Long, BookMeta>
    ) = coroutineScope {
        val csvFiles = Files.list(linksDir)
            .filter { it.fileName.toString().endsWith(".csv") }
            .toList()

        logger.i { "Processing ${csvFiles.size} link files..." }

        // Channel for collecting links from parallel processors
        val linkChannel = Channel<Link>(Channel.BUFFERED)

        // Launch parallel file processors
        val processors = csvFiles.map { file ->
            launch(Dispatchers.IO) {
                processLinkFile(
                    file = file,
                    refsByCanonical = refsByCanonical,
                    refsByBase = refsByBase,
                    lineKeyToId = lineKeyToId,
                    lineIdToBookId = lineIdToBookId,
                    bookMetaById = bookMetaById,
                    linkChannel = linkChannel
                )
            }
        }

        // Launch batch inserter
        val inserter = launch {
            val batch = mutableListOf<Link>()
            for (link in linkChannel) {
                batch += link
                if (batch.size >= SefariaImportTuning.LINK_BATCH_SIZE) {
                    repository.insertLinksBatch(batch)
                    batch.clear()
                }
            }
            // Flush remaining
            if (batch.isNotEmpty()) {
                repository.insertLinksBatch(batch)
            }
        }

        // Wait for all processors to finish
        processors.joinAll()
        linkChannel.close()

        // Wait for inserter to finish
        inserter.join()
    }

    private suspend fun processLinkFile(
        file: Path,
        refsByCanonical: Map<String, List<RefEntry>>,
        refsByBase: Map<String, RefEntry>,
        lineKeyToId: Map<Pair<String, Int>, Long>,
        lineIdToBookId: Map<Long, Long>,
        bookMetaById: Map<Long, BookMeta>,
        linkChannel: Channel<Link>
    ) {
        Files.newBufferedReader(file).use { reader ->
            val iter = reader.lineSequence().iterator()
            if (!iter.hasNext()) return
            val headers = parseCsvLine(iter.next()).map { normalizeCitation(it) }
            val idxC1 = headers.indexOf("Citation 1")
            val idxC2 = headers.indexOf("Citation 2")
            val idxConn = headers.indexOf("Conection Type")
            if (idxC1 < 0 || idxC2 < 0 || idxConn < 0) return

            while (iter.hasNext()) {
                val row = parseCsvLine(iter.next())
                if (row.isEmpty()) continue
                val c1 = normalizeCitation(row.getOrNull(idxC1).orEmpty())
                val c2 = normalizeCitation(row.getOrNull(idxC2).orEmpty())
                if (c1.isEmpty() || c2.isEmpty()) continue
                val conn = row.getOrNull(idxConn)?.trim().orEmpty()

                val fromRefs = resolveRefs(c1, refsByCanonical, refsByBase)
                val toRefs = resolveRefs(c2, refsByCanonical, refsByBase)
                if (fromRefs.isEmpty() || toRefs.isEmpty()) continue

                for (from in fromRefs) {
                    for (to in toRefs) {
                        val srcLine = lineKeyToId[from.path to (from.lineIndex - 1)] ?: continue
                        val tgtLine = lineKeyToId[to.path to (to.lineIndex - 1)] ?: continue
                        val baseConnectionType = ConnectionType.fromString(conn)
                        val (forwardType, reverseType) = resolveDirectionalConnectionTypes(
                            baseType = baseConnectionType,
                            sourceBookId = lineBookId(srcLine, lineIdToBookId),
                            targetBookId = lineBookId(tgtLine, lineIdToBookId),
                            bookMetaById = bookMetaById
                        )

                        // Send links to channel
                        linkChannel.send(
                            Link(
                                sourceBookId = lineBookId(srcLine, lineIdToBookId),
                                targetBookId = lineBookId(tgtLine, lineIdToBookId),
                                sourceLineId = srcLine,
                                targetLineId = tgtLine,
                                connectionType = forwardType
                            )
                        )

                        linkChannel.send(
                            Link(
                                sourceBookId = lineBookId(tgtLine, lineIdToBookId),
                                targetBookId = lineBookId(srcLine, lineIdToBookId),
                                sourceLineId = tgtLine,
                                targetLineId = srcLine,
                                connectionType = reverseType
                            )
                        )
                    }
                }
            }
        }
    }

    private fun lineBookId(lineId: Long, lineIdToBookId: Map<Long, Long>): Long =
        lineIdToBookId[lineId] ?: 0

    private fun resolveDirectionalConnectionTypes(
        baseType: ConnectionType,
        sourceBookId: Long,
        targetBookId: Long,
        bookMetaById: Map<Long, BookMeta>
    ): Pair<ConnectionType, ConnectionType> {
        if (baseType != ConnectionType.COMMENTARY && baseType != ConnectionType.TARGUM) {
            return baseType to baseType
        }

        val sourceMeta = bookMetaById[sourceBookId] ?: return baseType to baseType
        val targetMeta = bookMetaById[targetBookId] ?: return baseType to baseType

        fun typesFor(sourceIsSecondary: Boolean): Pair<ConnectionType, ConnectionType> {
            return when (baseType) {
                ConnectionType.COMMENTARY ->
                    if (sourceIsSecondary) {
                        ConnectionType.SOURCE to ConnectionType.COMMENTARY
                    } else {
                        ConnectionType.COMMENTARY to ConnectionType.SOURCE
                    }

                ConnectionType.TARGUM ->
                    if (sourceIsSecondary) {
                        ConnectionType.SOURCE to ConnectionType.TARGUM
                    } else {
                        ConnectionType.TARGUM to ConnectionType.SOURCE
                    }

                else -> baseType to baseType
            }
        }

        if (sourceMeta.isBaseBook && !targetMeta.isBaseBook) {
            return typesFor(sourceIsSecondary = false)
        }
        if (!sourceMeta.isBaseBook && targetMeta.isBaseBook) {
            return typesFor(sourceIsSecondary = true)
        }

        val sourceLevel = sourceMeta.categoryLevel
        val targetLevel = targetMeta.categoryLevel
        if (sourceLevel < targetLevel) {
            return typesFor(sourceIsSecondary = false)
        }
        if (targetLevel < sourceLevel) {
            return typesFor(sourceIsSecondary = true)
        }

        return if (sourceBookId > targetBookId) {
            typesFor(sourceIsSecondary = true)
        } else {
            typesFor(sourceIsSecondary = false)
        }
    }

    suspend fun updateBookHasLinks() {
        repository.executeRawQuery(
            "INSERT OR IGNORE INTO book_has_links(bookId, hasSourceLinks, hasTargetLinks) " +
                "SELECT id, 0, 0 FROM book"
        )
        repository.executeRawQuery("UPDATE book_has_links SET hasSourceLinks=0, hasTargetLinks=0")
        repository.executeRawQuery(
            "UPDATE book_has_links SET hasSourceLinks=1 " +
                "WHERE bookId IN (SELECT DISTINCT sourceBookId FROM link)"
        )
        repository.executeRawQuery(
            "UPDATE book_has_links SET hasTargetLinks=1 " +
                "WHERE bookId IN (SELECT DISTINCT targetBookId FROM link)"
        )

        repository.executeRawQuery(
            "UPDATE book SET hasTargumConnection=0, hasReferenceConnection=0, hasSourceConnection=0, hasCommentaryConnection=0, hasOtherConnection=0"
        )

        suspend fun setConnFlag(typeName: String, column: String) {
            val sql = "UPDATE book SET $column=1 WHERE id IN (" +
                "SELECT DISTINCT bId FROM (" +
                "SELECT sourceBookId AS bId FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId WHERE ct.name='$typeName' " +
                "UNION " +
                "SELECT targetBookId AS bId FROM link l JOIN connection_type ct ON ct.id = l.connectionTypeId WHERE ct.name='$typeName'" +
                ")" +
                ")"
            repository.executeRawQuery(sql)
        }

        setConnFlag("TARGUM", "hasTargumConnection")
        setConnFlag("REFERENCE", "hasReferenceConnection")
        setConnFlag("SOURCE", "hasSourceConnection")
        setConnFlag("COMMENTARY", "hasCommentaryConnection")
        setConnFlag("OTHER", "hasOtherConnection")
    }
}

