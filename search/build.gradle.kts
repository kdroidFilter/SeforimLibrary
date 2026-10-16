import org.jetbrains.kotlin.gradle.plugin.mpp.TestExecutable
import org.jetbrains.kotlin.gradle.targets.native.tasks.KotlinNativeSimulatorTest
import java.net.URI
import java.security.DigestInputStream
import java.security.MessageDigest

plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.android.library)
    alias(libs.plugins.kotlinx.serialization)
}

group = "io.github.kdroidfilter.seforimlibrary"

// The Java runtime Lucene runs in on iOS (OpenJDK's mobile port, see IOS_RUNTIME.md), built by the CI of
// kdroidFilter/openjdk-mobile (branch mobile-runtime) and downloaded from its release, checked: OpenJDK 27 GA (the JDK's
// build 35) with openjdk/mobile's port, its first release there (jdk-mobile-27b35.1)
val jdkMobileVersion = "27"
val jdkMobileBuild = "35"
val jdkMobileRevision = "1"
val jdkMobileRelease = "jdk-mobile-${jdkMobileVersion}b$jdkMobileBuild.$jdkMobileRevision"
val mobileRuntimeSha256 =
    mapOf(
        "ios-simulator" to "0471359fb8711b9638c6f4432d170d74f8ece94994fe613c84406f690d661ded",
        "ios-device" to "b5ca69777a83df475c2287ced5eb1df2fddefe2a9a235e9320ef0e488e509201",
    )

fun runtimeTaskSuffix(target: String): String = target.split("-").joinToString("") { it.replaceFirstChar(Char::uppercase) }

fun runtimeAsset(target: String): String = "jdk-mobile-$jdkMobileVersion-$target-aarch64-b$jdkMobileBuild.$jdkMobileRevision.tar.gz"

fun runtimeArchive(target: String) = layout.buildDirectory.file("javaRuntime/${runtimeAsset(target)}")

val downloadJavaRuntimes =
    mobileRuntimeSha256.mapValues { (target, sha256) ->
        val asset = runtimeAsset(target)
        val archive = runtimeArchive(target)
        tasks.register("downloadJavaRuntime${runtimeTaskSuffix(target)}") {
            description = "Downloads the Java runtime for $target ($jdkMobileRelease)."
            val url = "https://github.com/kdroidFilter/openjdk-mobile/releases/download/$jdkMobileRelease/$asset"
            inputs.property("url", url)
            inputs.property("sha256", sha256)
            outputs.file(archive)
            doLast {
                // Not a download for nothing: a release's checksums are set here once it is published
                check(sha256.matches(Regex("[0-9a-f]{64}"))) { "No SHA-256 for $asset yet: set it in mobileRuntimeSha256 (see IOS_RUNTIME.md)" }
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
    }

// build/javaRuntime/<target>: static/ (the libraries an app links), include/ (its JNI headers) and lib/ (its java.home)
val javaRuntimes =
    downloadJavaRuntimes.mapValues { (target, download) ->
        tasks.register<Sync>("javaRuntime${runtimeTaskSuffix(target)}") {
            description = "Extracts the Java runtime for $target."
            dependsOn(download)
            from(tarTree(runtimeArchive(target)))
            into(layout.buildDirectory.dir("javaRuntime/$target"))
            // Some of the JDK's files are read-only: the next sync couldn't replace them
            filePermissions { user { write = true } }
        }
    }

// The runtime's static libraries an iOS binary links (force-loaded: the runtime finds its natives by name)
val jdkStaticLibs =
    listOf(
        "libjvm.a", "libjava.a", "libnio.a", "libnet.a", "libzip.a", "libjimage.a", "libverify.a", "libextnet.a",
        "libfallbackLinker.a", "libsyslookup.a",
    )
val iosSimulatorRuntime = layout.buildDirectory.dir("javaRuntime/ios-simulator").get().asFile

// The JNI headers of each target's runtime, for its iOS bindings (cinterop "jni"): extracted alone (compiling needs
// nothing else of the runtime), in a directory per release, so a new runtime regenerates the bindings
val jniHeaders =
    downloadJavaRuntimes.mapValues { (target, download) ->
        tasks.register<Sync>("jniHeaders${runtimeTaskSuffix(target)}") {
            description = "Extracts the JNI headers of the Java runtime for $target."
            dependsOn(download)
            from(tarTree(runtimeArchive(target))) { include("include/**") }
            into(layout.buildDirectory.dir("jni/$jdkMobileRelease/$target"))
        }
    }

fun jniHeadersDir(target: String): File = layout.buildDirectory.dir("jni/$jdkMobileRelease/$target/include").get().asFile

// The runtime target of each Kotlin/Native target
val iosRuntimeTargets = mapOf("iosArm64" to "ios-device", "iosSimulatorArm64" to "ios-simulator")

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()
    android {
        namespace = "io.github.kdroidfilter.seforimlibrary.search"
        compileSdk = 35
        // Lucene 10 needs the Java 21 APIs ART only has from Android 15
        minSdk = 35
        // The search on ART, on a device or an emulator (the parity dump on a real index, see SearchDump)
        withDeviceTestBuilder { sourceSetTreeName = "test" }.configure {
            instrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        }
    }
    // The iOS tests link the runtime and run the search end to end in the simulator
    iosSimulatorArm64 {
        binaries.withType<TestExecutable>().configureEach {
            val static = iosSimulatorRuntime.resolve("static")
            linkerOpts(jdkStaticLibs.flatMap { listOf("-force_load", static.resolve(it).path) })
            linkerOpts(static.resolve("libffi.a").path, "-export_dynamic", "-lz", "-lc++")
            listOf("Foundation", "CoreFoundation", "Security", "SystemConfiguration").forEach { linkerOpts("-framework", it) }
            linkTaskProvider.configure { dependsOn(javaRuntimes.getValue("ios-simulator")) }
        }
    }
    listOf(iosArm64(), iosSimulatorArm64()).forEach { target ->
        target.compilations.getByName("main").cinterops {
            create("jni") {
                definitionFile.set(project.file("src/nativeInterop/cinterop/jni.def"))
                // The target's own runtime's
                includeDirs(jniHeadersDir(iosRuntimeTargets.getValue(target.name)))
            }
            create("sqlite3") {
                definitionFile.set(project.file("src/nativeInterop/cinterop/sqlite3.def"))
            }
        }
    }

    sourceSets {
        commonMain.dependencies {
            api(project(":core"))
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.serialization.cbor)
            implementation(libs.kotlinx.io.core)
            implementation(libs.atomicfu)
            implementation(libs.kermit)
        }

        // Lucene runs in process on the JVM and Android (ART, API 35+): the same code serves both
        val androidJvmMain by creating {
            dependsOn(commonMain.get())
            dependencies {
                compileOnly(libs.lucene.core)
                implementation(libs.jsoup)
            }
        }
        jvmMain {
            dependsOn(androidJvmMain)
            dependencies {
                implementation(libs.lucene.core)
                implementation(libs.lucene.analysis.common)
                implementation(libs.sqlite.jdbc)
            }
        }
        androidMain {
            dependsOn(androidJvmMain)
            dependencies {
                // lucene-core patched for ART
                implementation(project(":lucene-android"))
            }
        }

        // The manual source set above turns off the default hierarchy: iOS shares its own one by hand
        val iosMain by creating {
            dependsOn(commonMain.get())
            dependencies {
                implementation(libs.ksoup)
            }
        }
        iosArm64Main { dependsOn(iosMain) }
        iosSimulatorArm64Main { dependsOn(iosMain) }

        getByName("androidDeviceTest").dependencies {
            implementation(kotlin("test"))
            implementation(libs.androidx.test.runner)
            implementation(libs.androidx.test.junit)
        }

        jvmTest.dependencies {
            implementation(kotlin("test"))
            implementation(libs.kotlinx.coroutines.test)
        }
    }
}

iosRuntimeTargets.forEach { (konanTarget, runtimeTarget) ->
    tasks.matching { it.name == "cinteropJni${konanTarget.replaceFirstChar(Char::uppercase)}" }.configureEach {
        dependsOn(jniHeaders.getValue(runtimeTarget))
        // The headers' content, not only their directory: the bindings follow them
        inputs.dir(jniHeadersDir(runtimeTarget)).withPathSensitivity(PathSensitivity.RELATIVE)
    }
}

// The jars the embedded Java runtime runs: this module (its Lucene side and IndexBridgeServer), Lucene, Kotlin
val iosJvmClasspath by tasks.registering(Sync::class) {
    description = "Collects the jars the Java runtime of an iOS app runs for the search."
    from(tasks.named("jvmJar"))
    from(
        configurations.named("jvmRuntimeClasspath").map { classpath ->
            classpath.filter { jar -> listOf("lucene-core", "kotlin-stdlib", "kotlinx-serialization-core", "kotlinx-serialization-cbor").any { jar.name.startsWith(it) } }
        },
    )
    into(layout.buildDirectory.dir("iosJvmClasspath"))
}

run {
    val testBinaryDir = layout.buildDirectory.dir("bin/iosSimulatorArm64/debugTest")

    // java.home of the runtime is <executable dir>/lib: its classes (an exploded image), conf and data, the jars beside
    val iosTestRuntime by tasks.registering(Sync::class) {
        dependsOn(javaRuntimes.getValue("ios-simulator"))
        from(iosSimulatorRuntime.resolve("lib"))
        from(iosJvmClasspath) { into("app") }
        into(testBinaryDir.map { it.dir("lib") })
        filePermissions { user { write = true } }
    }

    tasks.withType<KotlinNativeSimulatorTest>().configureEach {
        dependsOn(iosTestRuntime)
        device.set(providers.gradleProperty("seforim.iosSimulator").orElse("iPhone 17 Pro"))
        val jars = iosJvmClasspath.map { sync -> sync.destinationDir.listFiles().orEmpty().map { testBinaryDir.get().dir("lib/app/${it.name}").asFile.path } }
        val indexDir = providers.gradleProperty("indexDir")
        val parityOut = layout.buildDirectory.file("parity/ios.txt")
        val luceneQueryCache = providers.gradleProperty("luceneQueryCache")
        doFirst {
            // The simulator passes the SIMCTL_CHILD_ variables to the test process, without the prefix
            environment("SIMCTL_CHILD_SEFORIM_JVM_CLASSPATH", jars.get().joinToString(":"))
            indexDir.orNull?.let { environment("SIMCTL_CHILD_SEFORIM_INDEX_DIR", it) }
            environment("SIMCTL_CHILD_SEFORIM_PARITY_OUT", parityOut.get().asFile.path)
            luceneQueryCache.orNull?.let { environment("SIMCTL_CHILD_SEFORIM_JVM_OPTIONS", "-Dseforim.lucene.queryCache=$it") }
        }
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
