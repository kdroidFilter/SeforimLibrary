import org.jetbrains.compose.desktop.application.dsl.TargetFormat

plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.compose.compiler)
    alias(libs.plugins.compose)
}

kotlin {
    jvmToolchain(17)
    jvm()

    sourceSets {
        commonMain.dependencies {
            implementation(compose.runtime)
            implementation(compose.foundation)
            implementation(compose.material)
            implementation(libs.kermit)
            implementation(project(":core"))
            implementation(project(":dao"))
            implementation("com.mohamedrejeb.richeditor:richeditor-compose:1.0.0-rc13")
            implementation("io.github.vinceglb:filekit-core:0.10.0-beta04")
            implementation("io.github.vinceglb:filekit-dialogs:0.10.0-beta04")
            implementation("io.github.vinceglb:filekit-dialogs-compose:0.10.0-beta04")
        }


        jvmMain.dependencies {
            implementation(compose.desktop.currentOs)
            implementation("app.cash.sqldelight:sqlite-driver:2.1.0")
            implementation("app.cash.sqldelight:jdbc-driver:2.1.0")
        }

    }
}

compose.desktop {
    application {
        mainClass = "MainKt"

        nativeDistributions {
            modules("java.sql")
            targetFormats(TargetFormat.Dmg, TargetFormat.Msi, TargetFormat.Deb)
            packageName = "sample"
            packageVersion = "1.0.0"
        }
    }
}
