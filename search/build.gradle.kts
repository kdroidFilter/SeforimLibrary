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
// kdroidFilter/openjdk-mobile (branch mobile-runtime) and downloaded from its release, checked
val mobileRuntimeRelease = "mobile-runtime-1"
val mobileRuntimeSha256 =
    mapOf(
        "ios-simulator" to "5f392f51cece76b0bc318bced666738e3ce16fa559a7a385d345ab3955458384",
        "ios-device" to "62ca2a74da57913613ae076905cb812d117afcec6472ffae3574488682fbf758",
    )

// build/javaRuntime/<target>: static/ (the libraries an app links) and lib/ (its java.home)
val javaRuntimes =
    mobileRuntimeSha256.mapValues { (target, sha256) ->
        val suffix = target.split("-").joinToString("") { it.replaceFirstChar(Char::uppercase) }
        val archive = layout.buildDirectory.file("javaRuntime/jdk-$target.tar.gz")
        val download =
            tasks.register("downloadJavaRuntime$suffix") {
                description = "Downloads the Java runtime for $target ($mobileRuntimeRelease)."
                val url = "https://github.com/kdroidFilter/openjdk-mobile/releases/download/$mobileRuntimeRelease/jdk-$target.tar.gz"
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
                        error("Unexpected jdk-$target.tar.gz from $url (SHA-256 $actual)")
                    }
                }
            }
        tasks.register<Sync>("javaRuntime$suffix") {
            description = "Extracts the Java runtime for $target."
            dependsOn(download)
            from(tarTree(archive))
            into(layout.buildDirectory.dir("javaRuntime/$target"))
            // Some of the JDK's files are read-only: the next sync couldn't replace them
            filePermissions { user { write = true } }
        }
    }

// The JNI headers of that runtime, for the iOS bindings (cinterop "jni"): from the sources of the same release
val jniHeaders = layout.buildDirectory.dir("jni/$mobileRuntimeRelease/include")
val downloadJniHeaders by tasks.registering {
    description = "Downloads the JNI headers of the Java runtime ($mobileRuntimeRelease) for the iOS bindings."
    val sources = "https://raw.githubusercontent.com/kdroidFilter/openjdk-mobile/$mobileRuntimeRelease/src/java.base"
    val headers =
        mapOf(
            "jni.h" to ("$sources/share/native/include/jni.h" to "1e03bd83d1662ac00647f7a06c11d32333767ee537803ec1d153e89888414520"),
            "jni_md.h" to ("$sources/unix/native/include/jni_md.h" to "8232a5a68d36060aaa426fbfdb4537983ecfdfc5fe07a43230005ddd1a4b986a"),
        )
    val dir = jniHeaders
    inputs.property("headers", headers.toString())
    outputs.dir(dir)
    doLast {
        headers.forEach { (name, source) ->
            val (url, sha256) = source
            val bytes = URI(url).toURL().readBytes()
            val actual = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
            check(actual == sha256) { "Unexpected $name from $url (SHA-256 $actual)" }
            dir.get().file(name).asFile.apply { parentFile.mkdirs() }.writeBytes(bytes)
        }
    }
}

// The runtime's static libraries an iOS binary links (force-loaded: the runtime finds its natives by name)
val jdkStaticLibs =
    listOf(
        "libjvm.a", "libjava.a", "libnio.a", "libnet.a", "libzip.a", "libjimage.a", "libverify.a", "libextnet.a",
        "libfallbackLinker.a", "libsyslookup.a",
    )
val iosSimulatorRuntime = layout.buildDirectory.dir("javaRuntime/ios-simulator").get().asFile

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
                includeDirs(jniHeaders)
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

tasks.matching { it.name.startsWith("cinteropJni") }.configureEach { dependsOn(downloadJniHeaders) }

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
