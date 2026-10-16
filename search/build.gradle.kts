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
// build 35) with openjdk/mobile's port, its second release there (jdk-mobile-27b35.2: x86_64 Android added)
val jdkMobileVersion = "27"
val jdkMobileBuild = "35"
val jdkMobileRevision = "2"
val jdkMobileRelease = "jdk-mobile-${jdkMobileVersion}b$jdkMobileBuild.$jdkMobileRevision"
val mobileRuntimeSha256 =
    mapOf(
        "ios-simulator" to "5cebc9571c2162832ded028a4c5ab916983f12b290c67ba03d314d64f6611361",
        "ios-device" to "b8d81c33fe36f73f70ed319500fdf00b5ff140f297764afa5b015891913c4456",
        "android-aarch64" to "0f5d13d3726896a30423f2bdae7d60083ec4a4f3bf71b72c4706677cf2a418e5",
        "android-x64" to "05e432d86b9f12ffa236b1c476b10102b520f7e085034dc0ffb8aa11f5d0cdd4",
    )

fun runtimeTaskSuffix(target: String): String = target.split("-").joinToString("") { it.replaceFirstChar(Char::uppercase) }

// The asset of a target, named like the JBR's archives: <platform>-<arch> (the iOS runtimes are arm64 only)
fun runtimeAsset(target: String): String {
    val platform = if (target.startsWith("ios-")) "$target-aarch64" else target
    return "jdk-mobile-$jdkMobileVersion-$platform-b$jdkMobileBuild.$jdkMobileRevision.tar.gz"
}

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

// build/javaRuntime/<target>: include/ (its JNI headers), lib/ (its java.home) and, on iOS, static/ (the libraries an app
// links), on Android jniLibs/ (the launcher of the Java process)
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
        // Lucene runs in a Java process of the app's own (JavaProcessTransport), from Android 10
        minSdk = 29
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

        // Lucene runs on a JVM only: in process on the desktop, in the Java runtime the mobile apps embed (a process of
        // the app's own on Android, in the app on iOS), behind the same bridge. The JVM and Android share the rest
        val androidJvmMain by creating {
            dependsOn(commonMain.get())
            dependencies {
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
val mobileJvmClasspath by tasks.registering(Sync::class) {
    description = "Collects the jars the Java runtime of a mobile app runs for the search (iOS, Android)."
    from(tasks.named("jvmJar"))
    from(
        configurations.named("jvmRuntimeClasspath").map { classpath ->
            classpath.filter { jar -> listOf("lucene-core", "kotlin-stdlib", "kotlinx-serialization-core", "kotlinx-serialization-cbor").any { jar.name.startsWith(it) } }
        },
    )
    into(layout.buildDirectory.dir("mobileJvmClasspath"))
}

run {
    val testBinaryDir = layout.buildDirectory.dir("bin/iosSimulatorArm64/debugTest")

    // java.home of the runtime is <executable dir>/lib: its classes (an exploded image), conf and data, the jars beside
    val iosTestRuntime by tasks.registering(Sync::class) {
        dependsOn(javaRuntimes.getValue("ios-simulator"))
        from(iosSimulatorRuntime.resolve("lib"))
        from(mobileJvmClasspath) { into("app") }
        into(testBinaryDir.map { it.dir("lib") })
        filePermissions { user { write = true } }
    }

    tasks.withType<KotlinNativeSimulatorTest>().configureEach {
        dependsOn(iosTestRuntime)
        device.set(providers.gradleProperty("seforim.iosSimulator").orElse("iPhone 17 Pro"))
        val jars = mobileJvmClasspath.map { sync -> sync.destinationDir.listFiles().orEmpty().map { testBinaryDir.get().dir("lib/app/${it.name}").asFile.path } }
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
