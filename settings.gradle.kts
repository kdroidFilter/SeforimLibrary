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

dependencyResolutionManagement {
    repositories {
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
include(":catalog")
include(":searchindex")
include(":packaging")
include(":sefariasqlite")
include(":otzariasqlite")

project(":catalog").projectDir = file("generator/catalog")
project(":searchindex").projectDir = file("generator/searchindex")
project(":packaging").projectDir = file("generator/packaging")
project(":sefariasqlite").projectDir = file("generator/sefariasqlite")
project(":otzariasqlite").projectDir = file("generator/otzariasqlite")

includeBuild("SeforimMagicIndexer")
