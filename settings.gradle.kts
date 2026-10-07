rootProject.name = "SeforimLibrary"

pluginManagement {
    repositories {
        google {
            content { 
              	includeGroupByRegex("com\\.android.*")
              	includeGroupByRegex("com\\.google.*")
              	includeGroupByRegex("androidx.*")
              	includeGroupByRegex("android.*")
            }
        }
        gradlePluginPortal()
        mavenCentral()
    }
}

/**
 * The token that reads the semantic search's private package (open core): with it, the generator embeds the corpus into
 * the index's dense vectors. semantic.token, SEFORIM_SEMANTIC_TOKEN, the siddur's token (siddur.token,
 * SEFORIM_SIDDUR_TOKEN) as in Zayit, or the gh CLI's when it can read the package; none in a community build.
 */
val semanticToken: String? =
    providers.gradleProperty("semantic.token").orNull
        ?: providers.environmentVariable("SEFORIM_SEMANTIC_TOKEN").orNull
        ?: providers.gradleProperty("siddur.token").orNull
        ?: providers.environmentVariable("SEFORIM_SIDDUR_TOKEN").orNull
        ?: run {
            val gh = providers.environmentVariable("PATH").getOrElse("").split(File.pathSeparator)
                .flatMap { listOf(File(it, "gh"), File(it, "gh.exe")) }
                .firstOrNull { it.canExecute() }
                ?.path
            fun ghOutput(vararg args: String): String? =
                gh?.let {
                    val exec = providers.exec {
                        commandLine(it, *args)
                        isIgnoreExitValue = true
                    }
                    exec.standardOutput.asText.get().trim().takeIf { exec.result.get().exitValue == 0 }
                }
            ghOutput("api", "users/kdroidFilter/packages/maven/io.github.kdroidfilter.seforim-semantic", "--jq", ".name")
                ?.let { ghOutput("auth", "token") }
        }
gradle.extensions.extraProperties["semanticEnabled"] = semanticToken != null

dependencyResolutionManagement {
    repositories {
        // The semantic search, for the official builds (open core): a private package, read with a token
        semanticToken?.let { token ->
            maven("https://maven.pkg.github.com/kdroidFilter/SeforimEmbedding") {
                credentials {
                    username = "token"
                    password = token
                }
                content { includeModuleByRegex("io\\.github\\.kdroidfilter", "seforim-semantic.*") }
            }
        }
        google {
            content { 
              	includeGroupByRegex("com\\.android.*")
              	includeGroupByRegex("com\\.google.*")
              	includeGroupByRegex("androidx.*")
              	includeGroupByRegex("android.*")
            }
        }
        mavenCentral()
    }
}
include(":core")
include(":dao")
include(":search")
include(":cli")
include(":catalog")
include(":searchindex")
include(":packaging")
include(":sefariasqlite")
include(":otzariasqlite")
include(":generator-common")
include(":delta-updater")

project(":catalog").projectDir = file("generator/catalog")
project(":searchindex").projectDir = file("generator/searchindex")
project(":packaging").projectDir = file("generator/packaging")
project(":sefariasqlite").projectDir = file("generator/sefariasqlite")
project(":otzariasqlite").projectDir = file("generator/otzariasqlite")
project(":generator-common").projectDir = file("generator/common")

includeBuild("SeforimMagicIndexer")
