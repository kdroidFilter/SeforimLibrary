package io.github.kdroidfilter.seforimlibrary.reflinks

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.reflinks.arbitration.DecisionStore
import io.github.kdroidfilter.seforimlibrary.reflinks.arbitration.Prompts
import io.github.kdroidfilter.seforimlibrary.reflinks.corpus.CitationScanner
import io.github.kdroidfilter.seforimlibrary.reflinks.corpus.HeRefIndex
import io.github.kdroidfilter.seforimlibrary.reflinks.corpus.Method
import io.github.kdroidfilter.seforimlibrary.reflinks.corpus.Occurrence
import io.github.kdroidfilter.seforimlibrary.reflinks.corpus.SefariaSchemas
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.sql.DriverManager

/**
 * Writes the citation links into the text of the database: `<a class="ref" href="seforim://line/<id>">`.
 * Deterministic links plus the LLM decisions already in the cache; never calls the network.
 * Writes in place, or into a copy with `--out` (the source is then never modified).
 *
 * Usage: --db=<seforim.db> [--out=<copy to write>] [--decisions=…] [--sefaria-schemas=…]
 */
fun main(args: Array<String>) {
    Logger.setMinSeverity(Severity.Info)
    val log = Logger.withTag("GenerateRefLinks")
    val opts = args.filter { it.startsWith("--") }
        .associate { it.removePrefix("--").substringBefore('=') to it.substringAfter('=', "") }
    val source = Path.of(requireNotNull(opts["db"]) { "--db is required" })
    val out = opts["out"]?.let(Path::of) ?: source
    val inPlace = source.toAbsolutePath().normalize() == out.toAbsolutePath().normalize()
    val decisions = DecisionStore.open(Path.of(opts["decisions"] ?: "generator/data/ref-arbitration.jsonl"))
    val sefariaRefs = SefariaRefParser(SefariaSchemas.load(Path.of(opts["sefaria-schemas"] ?: DEFAULT_SEFARIA_SCHEMAS)))
    // A second pass would scan the links of the first one as citations
    require(!hasRefLinks(source)) { "$source already has ref links: regenerate it, or write a copy of a DB without them" }

    // 1. Collect the links per line
    val links = HashMap<Long, MutableList<LinkInjector.Link>>()
    val counts = sortedMapOf<String, Int>()
    HeRefIndex(source.toString()).use { index ->
        CitationScanner(index, sefariaRefs).scan { occ ->
            val link = when (occ) {
                is Occurrence.Linked -> LinkInjector.Link(
                    occ.start, occ.end, occ.target.lineId,
                    existingAnchor = occ.method == Method.OTZARIA_ANCHOR || occ.method == Method.SEFARIA_ANCHOR,
                    arbitrated = false,
                ).also { counts.merge(occ.method.name, 1, Int::plus) }

                is Occurrence.Ambiguous -> {
                    val decision = decisions[occ.case.cacheKey(DEFAULT_MODEL, Prompts.VERSION)]
                    val target = decision?.target?.let { t -> occ.case.candidates.firstOrNull { it.target == t } }
                    target?.let {
                        counts.merge("LLM_" + occ.caseClass.name, 1, Int::plus)
                        LinkInjector.Link(occ.start, occ.end, it.lineId, existingAnchor = false, arbitrated = true)
                    }
                }
            }
            link?.let { links.getOrPut(occ.lineId) { mutableListOf() } += it }
        }
    }
    decisions.close()
    log.i { "Links found: $counts (total ${counts.values.sum()}) in ${links.size} lines" }

    // 2. Write them into the database, or into its copy
    if (!inPlace) {
        Files.createDirectories(out.toAbsolutePath().parent)
        log.i { "Copying $source → $out" }
        Files.copy(source, out, StandardCopyOption.REPLACE_EXISTING)
    }
    var written = 0
    DriverManager.getConnection("jdbc:sqlite:$out").use { db ->
        db.autoCommit = false
        db.prepareStatement("SELECT content FROM line WHERE id = ?").use { select ->
            db.prepareStatement("UPDATE line SET content = ? WHERE id = ?").use { update ->
                var batch = 0
                for ((lineId, lineLinks) in links) {
                    select.setLong(1, lineId)
                    val html = select.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null } ?: continue
                    val (newHtml, n) = LinkInjector.inject(html, lineLinks)
                    if (n == 0) continue
                    written += n
                    update.setString(1, newHtml)
                    update.setLong(2, lineId)
                    update.addBatch()
                    if (++batch % 10_000 == 0) {
                        update.executeBatch()
                        db.commit()
                    }
                }
                update.executeBatch()
                db.commit()
            }
        }
    }
    log.i { "Wrote $written links into $out" }
}

private fun hasRefLinks(db: Path): Boolean =
    DriverManager.getConnection("jdbc:sqlite:$db").use { conn ->
        conn.createStatement().use { st ->
            st.executeQuery("SELECT EXISTS(SELECT 1 FROM line WHERE instr(content, 'class=\"ref\"') > 0)")
                .use { it.next() && it.getBoolean(1) }
        }
    }
