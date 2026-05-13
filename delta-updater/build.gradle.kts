plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
}

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            api(project(":dao"))
            api(project(":generator-common"))
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kermit)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmMain.dependencies {
            implementation(libs.sqlite.jdbc)
            implementation(libs.sqlDelight.driver.sqlite)
            implementation(libs.zstd)
        }

        jvmTest.dependencies {
            implementation(kotlin("test-junit"))
            implementation(libs.sqlDelight.driver.sqlite)
        }
    }
}
