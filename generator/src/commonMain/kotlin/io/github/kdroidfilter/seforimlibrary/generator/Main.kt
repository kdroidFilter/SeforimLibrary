package io.github.kdroidfilter.seforimlibrary.generator

import app.cash.sqldelight.driver.jdbc.sqlite.JdbcSqliteDriver
import io.github.kdroidfilter.seforimlibrary.dao.repository.SeforimRepository
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.Path
import kotlin.system.exitProcess

fun main() = runBlocking {

    val dbFile = File("otzaria.db")
    val dbExists = dbFile.exists()
    println("DEBUG: Database file exists: $dbExists")

    // If the database file exists, rename it to make sure we're creating a new one
    if (dbExists) {
        val backupFile = File("otzaria.db.bak")
        if (backupFile.exists()) {
            backupFile.delete()
        }
        dbFile.renameTo(backupFile)
        println("DEBUG: Renamed existing database to otzaria.db.bak")
    }

    val driver = JdbcSqliteDriver(url = "jdbc:sqlite:otzaria.db")

    val sourcePath = Path("/home/elie-gambache/Téléchargements/otzaria (2)")
    val dbPath = Paths.get("otzaria.db").toFile().path

    if (!sourcePath.toFile().exists()) {
        println("Erreur: Le répertoire source n'existe pas: $sourcePath")
        exitProcess(1)
    }

    println("=== Générateur de base de données Otzaria ===")
    println("Source: $sourcePath")
    println("Base de données: $dbPath")
    println()

    val repository = SeforimRepository(dbPath, driver)

    try {
        val generator = DatabaseGenerator(sourcePath, repository)
        generator.generate()

        println("\nGénération terminée avec succès!")
        println("Base de données créée: $dbPath")
    } catch (e: Exception) {
        println("\nErreur lors de la génération:")
        e.printStackTrace()
        exitProcess(1)
    } finally {
        repository.close()
    }
}
