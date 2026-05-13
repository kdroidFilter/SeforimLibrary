plugins {
    alias(libs.plugins.multiplatform)
}

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kermit)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmMain.dependencies {
            implementation(libs.sqlite.jdbc)
            implementation(libs.kotlinx.coroutines.core)
            implementation(project(":dao"))
            implementation(libs.jsoup)
        }

        jvmTest.dependencies {
            implementation(kotlin("test-junit"))
            implementation(libs.sqlDelight.driver.sqlite)
        }
    }
}
