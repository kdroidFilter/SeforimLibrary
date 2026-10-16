import org.jetbrains.kotlin.gradle.targets.native.tasks.KotlinNativeSimulatorTest
import java.net.URI
import java.security.DigestInputStream
import java.security.MessageDigest

plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.android.library)
    alias(libs.plugins.kotlinx.serialization)
    alias(libs.plugins.nja)
}

group = "io.github.kdroidfilter.seforimlibrary"

// The Java runtimes Lucene runs in on the mobile platforms (see JVM_RUNTIME.md): jdk-mobile's releases, built by the CI
// of NucleusFramework/openjdk-mobile, downloaded and checked: OpenJDK 27 GA (the JDK's build 35) with openjdk/mobile's
// port, its third release there (jdk-mobile-27b35.3: the Android runtimes' static libraries, for NJA's in-process VM)
val jdkMobileVersion = "27"
val jdkMobileBuild = "35"
val jdkMobileRevision = "3"
val jdkMobileRelease = "jdk-mobile-${jdkMobileVersion}b$jdkMobileBuild.$jdkMobileRevision"
val jdkMobileSha256 =
    mapOf(
        "android-aarch64" to "625d0dcdb36f1e935b06ae5720169e77145500c23987a38e85600a4093ea4396",
        "android-x64" to "e6f69c83dd4c04a1822a53815d1a961d510b5b471c96313ee39e7a257148c654",
        "ios-simulator-aarch64" to "14043e85df2a019cba5ad55ac19979f4cacbfbd18ada8b2aeb895b52be0d2174",
        "ios-device-aarch64" to "e38dc41f3a85b2f0263692030e03228b4d3127d1da07f52a77215b5ce7d962d6",
    )

// build/jdkMobile/<target>, the layout NJA reads (nucleusJvm.jdkMobile): static/, include/ and lib/ (java.home)
val jdkMobileDir = layout.buildDirectory.dir("jdkMobile")
val stageJdkMobile =
    jdkMobileSha256.map { (target, sha256) ->
        val asset = "jdk-mobile-$jdkMobileVersion-$target-b$jdkMobileBuild.$jdkMobileRevision.tar.gz"
        val archive = layout.buildDirectory.file("jdkMobileDownloads/$asset")
        val suffix = target.split("-").joinToString("") { it.replaceFirstChar(Char::uppercase) }
        val download =
            tasks.register("downloadJdkMobile$suffix") {
                description = "Downloads jdk-mobile's runtime for $target ($jdkMobileRelease)."
                val url = "https://github.com/NucleusFramework/openjdk-mobile/releases/download/$jdkMobileRelease/$asset"
                inputs.property("url", url)
                inputs.property("sha256", sha256)
                outputs.file(archive)
                doLast {
                    val file = archive.get().asFile.apply { parentFile.mkdirs() }
                    val digest = MessageDigest.getInstance("SHA-256")
                    DigestInputStream(URI(url).toURL().openStream(), digest).use { input ->
                        file.outputStream().use { input.copyTo(it) }
                    }
                    val actual = digest.digest().joinToString("") { "%02x".format(it) }
                    if (actual != sha256) {
                        file.delete()
                        error("Unexpected $asset from $url (SHA-256 $actual)")
                    }
                }
            }
        tasks.register<Sync>("stageJdkMobile$suffix") {
            description = "Extracts jdk-mobile's runtime for $target."
            dependsOn(download)
            from(tarTree(archive))
            into(jdkMobileDir.map { it.dir(target) })
            // Some of the JDK's files are read-only: the next sync couldn't replace them
            filePermissions { user { write = true } }
        }
    }

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
    jdkMobile.set(jdkMobileDir)
    androidAbis.set(setOf("arm64-v8a", "x86_64"))
    // Lucene maps its files with Panama (madvise): native access, without the warning
    jvmOptions.addAll("-Xmx128m", "--enable-native-access=ALL-UNNAMED")
    // Lucene's query cache off: the same search ranks the same every time (the parity tests across platforms)
    providers.gradleProperty("luceneQueryCache").orNull?.let { jvmOptions.add("-Dseforim.lucene.queryCache=$it") }
}

// NJA reads the runtimes from jdkMobile, and the iOS binaries link them: staged first
tasks.matching { it.name.startsWith("nja") }.configureEach { dependsOn(stageJdkMobile) }
tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinNativeLink>().configureEach { dependsOn(stageJdkMobile) }

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
