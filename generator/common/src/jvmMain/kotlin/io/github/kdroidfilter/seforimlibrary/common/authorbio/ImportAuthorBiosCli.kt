package io.github.kdroidfilter.seforimlibrary.common.authorbio

import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import io.github.kdroidfilter.seforimlibrary.common.licenses.Licenses
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.util.zip.ZipInputStream
import kotlin.io.path.extension
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readText

private const val BIO_ARCHIVE_URL = "https://github.com/kdroidFilter/seforim-author-bio/archive/refs/heads/main.zip"

/**
 * Imports the author biographies of kdroidFilter/seforim-author-bio into `author_bio`.
 *
 * Runs after every step that creates authors. Each biography is joined to its author by
 * name (the author id's natural key), ignoring invisible bidi marks; biographies whose author is absent from this build
 * are reported and skipped. The biographies are CC BY-SA, so every row points at that license.
 *
 *   - `dbPath`        seforim.db to fill
 *   - `authorBioDir`  optional local checkout; the `main` branch archive is downloaded otherwise
 */
fun main() {
    Logger.setMinSeverity(Severity.Info)
    val logger = Logger.withTag("ImportAuthorBios")

    val dbPath = Paths.get(System.getProperty("dbPath") ?: error("-PdbPath= missing"))
    require(Files.isRegularFile(dbPath)) { "Database file not found: $dbPath" }
    val bios = System.getProperty("authorBioDir")?.let { readLocalBios(Paths.get(it)) } ?: downloadBios()
    logger.i { "Read ${bios.size} biographies" }

    DriverManager.getConnection("jdbc:sqlite:${dbPath.toAbsolutePath()}").use { conn ->
        conn.autoCommit = false
        val licenseId = conn.licenseId(Licenses.CC_BY_SA)
        val authorIds = conn.authorIdsByName()
        conn.createStatement().use { it.executeUpdate("DELETE FROM author_bio") }
        val unmatched = mutableListOf<String>()
        conn.prepareStatement(
            "INSERT INTO author_bio (authorId, summary, bodyMd, confidence, licenseId) VALUES (?, ?, ?, ?, ?)",
        ).use { ps ->
            bios.forEach { bio ->
                val authorId = authorIds[joinKey(bio.name)] ?: run {
                    unmatched += bio.name
                    return@forEach
                }
                ps.setLong(1, authorId)
                ps.setString(2, bio.summary)
                ps.setString(3, bio.bodyMd)
                ps.setString(4, bio.confidence)
                ps.setLong(5, licenseId)
                ps.addBatch()
            }
            ps.executeBatch()
        }
        conn.commit()
        logger.i { "Imported ${bios.size - unmatched.size} biographies for ${authorIds.size} authors" }
        if (unmatched.isNotEmpty()) logger.w { "No author named like these biographies: $unmatched" }
    }
}

private fun readLocalBios(root: Path): List<AuthorBioFile> {
    val dir = root.resolve("authors")
    require(dir.isDirectory()) { "No authors/ directory in $root" }
    return dir.listDirectoryEntries("*.md").sorted().map { parseOrFail(it.fileName.toString(), it.readText()) }
}

private fun downloadBios(): List<AuthorBioFile> =
    ZipInputStream(URI(BIO_ARCHIVE_URL).toURL().openStream()).use { zip ->
        generateSequence { zip.nextEntry }
            .filter { !it.isDirectory && it.name.substringBeforeLast('/').endsWith("/authors") }
            .filter { Path.of(it.name).extension == "md" }
            .map { parseOrFail(it.name, zip.readBytes().decodeToString()) }
            .toList()
    }

private fun parseOrFail(name: String, text: String): AuthorBioFile =
    runCatching { AuthorBioFile.parse(text) }.getOrElse { throw IllegalStateException("$name: ${it.message}", it) }

private fun Connection.licenseId(code: String): Long =
    prepareStatement("SELECT id FROM license WHERE code = ?").use { ps ->
        ps.setString(1, code)
        ps.executeQuery().use { rs ->
            check(rs.next()) { "License $code missing: run the Sefaria import first" }
            rs.getLong(1)
        }
    }

private fun Connection.authorIdsByName(): Map<String, Long> =
    createStatement().use { st ->
        st.executeQuery("SELECT id, name FROM author").use { rs ->
            buildMap { while (rs.next()) put(joinKey(rs.getString(2)), rs.getLong(1)) }
        }
    }

// Upstream names sometimes carry trailing LRM/RLM marks that the biography files dropped.
private val BIDI_MARKS = Regex("[\u200E\u200F\u202A-\u202E]")

private fun joinKey(name: String): String = name.replace(BIDI_MARKS, "").trim()
