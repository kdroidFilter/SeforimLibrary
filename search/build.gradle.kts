import org.jetbrains.kotlin.gradle.targets.native.tasks.KotlinNativeSimulatorTest

plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.android.library)
    alias(libs.plugins.kotlinx.serialization)
    alias(libs.plugins.nja)
}

group = "io.github.kdroidfilter.seforimlibrary"

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()
    android {
        namespace = "io.github.kdroidfilter.seforimlibrary.search"
        compileSdk = 35
        // NJA's bridge runs from Android 10
        minSdk = 29
    }
    listOf(iosArm64(), iosSimulatorArm64()).forEach { target ->
        target.compilations.getByName("main").cinterops {
            create("sqlite3") {
                definitionFile.set(project.file("src/nativeInterop/cinterop/sqlite3.def"))
            }
        }
    }

    sourceSets {
        commonMain.dependencies {
            api(project(":core"))
            implementation(libs.kotlinx.coroutines.core)
            // SearchFacets, which the apps keep
            api(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.io.core)
            implementation(libs.atomicfu)
            implementation(libs.kermit)
        }

        // The JVM and Android share the HTML and coroutine helpers (Lucene runs behind the façade, on a JVM)
        val androidJvmMain by creating {
            dependsOn(commonMain.get())
            dependencies {
                implementation(libs.jsoup)
            }
        }
        jvmMain {
            dependsOn(androidJvmMain)
            dependencies {
                implementation(libs.sqlite.jdbc)
            }
        }
        androidMain { dependsOn(androidJvmMain) }

        // The manual source set above turns off the default hierarchy: iOS shares its own one by hand
        val iosMain by creating {
            dependsOn(commonMain.get())
            dependencies {
                implementation(libs.ksoup)
            }
        }
        iosArm64Main { dependsOn(iosMain) }
        iosSimulatorArm64Main { dependsOn(iosMain) }

        jvmTest.dependencies {
            implementation(kotlin("test"))
            implementation(libs.kotlinx.coroutines.test)
            // The test indexes are written with Lucene directly
            implementation(libs.lucene.core)
            implementation(libs.lucene.analysis.common)
        }
    }
}

// Lucene, through Nucleus JVM Access: the façade (search-facade) runs on a JVM, the desktop's own or the one the mobile
// apps embed (iOS: Zero linked into the app; Android: HotSpot in the app's process), and common code uses its proxies
nucleusJvm {
    facade(project(":search-facade"))
    // The runtimes are jdk-mobile's latest release, which NJA downloads (see JVM_RUNTIME.md)
    androidAbis.set(setOf("arm64-v8a", "x86_64"))
    // Lucene maps its files with Panama (madvise): native access, without the warning
    jvmOptions.addAll("-Xmx128m", "--enable-native-access=ALL-UNNAMED")
    // Lucene's query cache off: the same search ranks the same every time (the parity tests across platforms)
    providers.gradleProperty("luceneQueryCache").orNull?.let { jvmOptions.add("-Dseforim.lucene.queryCache=$it") }
}

tasks.withType<KotlinNativeSimulatorTest>().configureEach {
    device.set(providers.gradleProperty("seforim.iosSimulator").orElse("iPhone 17 Pro"))
    val indexDir = providers.gradleProperty("indexDir")
    val parityOut = layout.buildDirectory.file("parity/ios.txt")
    doFirst {
        // The simulator passes the SIMCTL_CHILD_ variables to the test process, without the prefix
        indexDir.orNull?.let { environment("SIMCTL_CHILD_SEFORIM_INDEX_DIR", it) }
        environment("SIMCTL_CHILD_SEFORIM_PARITY_OUT", parityOut.get().asFile.path)
    }
}

tasks.named<Test>("jvmTest") {
    // The parity dump on a real index: -PindexDir=<seforim.db.lucene>
    providers.gradleProperty("indexDir").orNull?.let { systemProperty("seforimIndexDir", it) }
    systemProperty("parityOut", layout.buildDirectory.file("parity/jvm.txt").get().asFile.path)
    providers.gradleProperty("parityCores").orNull?.let { jvmArgs("-XX:ActiveProcessorCount=$it") }
    providers.gradleProperty("luceneQueryCache").orNull?.let { systemProperty("seforim.lucene.queryCache", it) }
    providers.gradleProperty("probeOut").orNull?.let { systemProperty("probeOut", it) }
    testLogging.showStandardStreams = true
}
