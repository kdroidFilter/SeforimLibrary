import org.jetbrains.compose.desktop.application.dsl.TargetFormat

plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.compose.compiler)
    alias(libs.plugins.compose)
    alias(libs.plugins.android.application)
}

kotlin {
    jvmToolchain(17)
    jvm()
    androidTarget()

    sourceSets {
        commonMain.dependencies {
            implementation(compose.runtime)
            implementation(compose.foundation)
            implementation(compose.material)
            implementation(compose.materialIconsExtended)
            implementation(libs.kermit)
            implementation(project(":core"))
            implementation(project(":dao"))
            implementation("com.mohamedrejeb.richeditor:richeditor-compose:1.0.0-rc13")
            implementation("io.github.vinceglb:filekit-core:0.10.0-beta04")
            implementation("io.github.vinceglb:filekit-dialogs:0.10.0-beta04")
            implementation("io.github.vinceglb:filekit-dialogs-compose:0.10.0-beta04")
            implementation("androidx.sqlite:sqlite:2.5.0-alpha01")
            implementation("androidx.sqlite:sqlite-bundled:2.5.0-alpha01")
            implementation("com.eygraber:sqldelight-androidx-driver:0.0.13")

        }


        jvmMain.dependencies {
            implementation(compose.desktop.currentOs)
            implementation("app.cash.sqldelight:sqlite-driver:2.1.0")
            implementation("app.cash.sqldelight:jdbc-driver:2.1.0")
        }

        androidMain.dependencies {
            implementation("app.cash.sqldelight:android-driver:2.1.0")
            implementation("androidx.activity:activity-compose:1.8.2")
            implementation("androidx.appcompat:appcompat:1.6.1")

        }

    }
}

android {
    namespace = "sample.app"
    compileSdk = 35

    defaultConfig {
        minSdk = 24
        targetSdk = 34
        versionCode = 1
        versionName = "1.0.0"
    }

    buildTypes {
        release {
            isMinifyEnabled = false
        }
    }
}

compose.desktop {
    application {
        mainClass = "MainKt"

        nativeDistributions {
            modules("java.sql", "jdk.security.auth")
            targetFormats(TargetFormat.Dmg, TargetFormat.Msi, TargetFormat.Deb)
            packageName = "sample"
            packageVersion = "1.0.0"
        }
    }
}
