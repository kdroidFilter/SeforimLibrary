import io.github.kdroidfilter.buildsrc.NativeCleanupTransformHelper
import org.jetbrains.compose.desktop.application.dsl.TargetFormat

plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
    alias(libs.plugins.compose)
    alias(libs.plugins.compose.compiler)
}

group = "io.github.kdroidfilter.seforimlibrary"
version = "1.0.0"

val mainClassName = "io.github.kdroidfilter.seforimlibrary.cli.MainKt"
val appName = "seforim-cli"

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        jvmMain.dependencies {
            implementation(project(":core"))
            implementation(project(":dao"))
            implementation(project(":search"))
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.sqlDelight.driver.sqlite)
            implementation(libs.kermit)
            implementation(libs.jsoup)
            implementation(libs.filekit.core)
            compileOnly(libs.compose.runtime)
        }

        jvmTest.dependencies {
            implementation(kotlin("test"))
        }
    }
}

compose.desktop {
    application {
        mainClass = mainClassName

        nativeDistributions {
            targetFormats(TargetFormat.Dmg, TargetFormat.Msi, TargetFormat.Deb)
            packageName = appName
            packageVersion = version.toString()
            jvmArgs("--enable-native-access=ALL-UNNAMED")
        }
    }
}

// Clean unused native binaries from JARs for smaller distribution size
NativeCleanupTransformHelper.registerTransform(project)
