import java.net.URI
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest

plugins {
    alias(libs.plugins.multiplatform)
}

// Generator forked-JVM heap. Honors -PgeneratorHeap=… (CI lowers it on 16 GB runners).
// Default 10g matches local workstation use; CI sets 5g via the workflow.
val seforimDb: String = (project.findProperty("seforimDb") as String?)
    ?: System.getenv("SEFORIM_DB")
    ?: rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath

// The semantic search (open core): with the token of its private package (settings.gradle.kts), the corpus is embedded
// into the index's dense vectors. -PvectorsBin=<dir> brings precomputed ones instead; -PnoVectors=true leaves them out.
val withSemantic = gradle.extensions.extraProperties.run { has("semanticEnabled") && get("semanticEnabled") == true }
val explicitVectors = project.findProperty("vectorsBin") as String?
val embedVectors = withSemantic && explicitVectors == null && project.findProperty("noVectors") != "true"
val vectorsDir: File = rootProject.layout.buildDirectory.dir("vectors").get().asFile

// Embedding runs on an NVIDIA GPU (Linux x64), with the CUDA runtime fetched below; elsewhere on the CPU, for hours
val cuda = embedVectors &&
    System.getProperty("os.name").startsWith("Linux") &&
    System.getProperty("os.arch") in setOf("amd64", "x86_64") &&
    File("/proc/driver/nvidia/version").exists()

// The vectors (~5.5 GB) are loaded in the indexer's heap
val generatorHeap: String = (project.findProperty("generatorHeap") as String?)
    ?: System.getenv("SEFORIM_GENERATOR_HEAP")
    ?: if (embedVectors || explicitVectors != null) "20g" else "10g"


kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kermit)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmMain.dependencies {
            implementation(project(":core"))
            implementation(project(":dao"))
            implementation(libs.sqlDelight.driver.sqlite)
            implementation(libs.jsoup)

            api(libs.lucene.core)
            api(libs.lucene.analysis.common)
        }
    }
}

// Build Lucene index using StandardAnalyzer
// Usage:
//   ./gradlew :searchindex:buildLuceneIndexDefault -PseforimDb=/path/to/seforim.db
tasks.register<JavaExec>("buildLuceneIndexDefault") {
    group = "application"
    description = "Build Lucene index using StandardAnalyzer. Requires -PseforimDb."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.searchindex.BuildLuceneIndexKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Pass DB path as system property recognized by the Kotlin entrypoint
    systemProperty("seforimDb", seforimDb)

    // Prefer in-memory DB for faster reads (override with -PinMemoryDb=false)
    val inMemory = project.findProperty("inMemoryDb") != "false"
    if (inMemory) {
        systemProperty("inMemoryDb", "true")
    }

    // Dense vectors (dir with ids.i64+vecs.f32+meta.txt) → SINGLE fused index (text + KnnFloatVectorField per line):
    // -PvectorsBin=/path, or the corpus embedded by embedCorpus
    explicitVectors?.let { systemProperty("vectorsBin", it) }
    if (embedVectors) {
        dependsOn("embedCorpus")
        systemProperty("vectorsBin", vectorsDir.absolutePath)
    }
    // Optional: -PindexThreads=N to cap concurrent indexing threads (lower = less RAM).
    (project.findProperty("indexThreads") as String?)?.let { systemProperty("indexThreads", it) }

    jvmArgs = listOf(
        "-Xmx$generatorHeap",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=200",
        "--enable-native-access=ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector"
    )
}

if (embedVectors) {
    val semanticEmbedder = configurations.create("semanticEmbedder") {
        isCanBeConsumed = false
        attributes { attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME)) }
        // onnxruntime_gpu: the same API, plus the CUDA execution provider
        if (cuda) {
            resolutionStrategy.dependencySubstitution {
                substitute(module("com.microsoft.onnxruntime:onnxruntime")).using(module(libs.onnxruntime.gpu.get().toString()))
            }
        }
    }
    dependencies {
        add(semanticEmbedder.name, libs.seforim.semantic)
        // The fp32 model, for the GPU (the int8 one, in seforim-semantic, has no CUDA kernels for its quantized ops)
        if (cuda) add(semanticEmbedder.name, libs.seforim.semantic.fp32)
    }

    // CUDA 12 + cuDNN 9 for onnxruntime_gpu, from NVIDIA's redistributables (no CUDA install): only their shared
    // libraries, in the Gradle user home, fetched once (~2 GB). -PcudaArchivesDir=<dir> takes the archives already
    // there instead of downloading them (still checked against their sha256)
    val localArchives = (project.findProperty("cudaArchivesDir") as String?)?.let(::File)
    val cudaLibDir = gradle.gradleUserHomeDir.resolve("caches/seforim-cuda/cuda-12.8-cudnn-9.10/lib")
    val prepareCudaRuntime = tasks.register("prepareCudaRuntime") {
        val archives = mapOf(
            "cuda/redist/cuda_cudart/linux-x86_64/cuda_cudart-linux-x86_64-12.8.90-archive.tar.xz" to
                "8d566b5fe745c46842dc16945cf36686227536decd2302c372be86da37faca68",
            "cuda/redist/libcublas/linux-x86_64/libcublas-linux-x86_64-12.8.4.1-archive.tar.xz" to
                "21718957c2cf000bacd69d36c95708a2319199e39e056f8b4f0f68e3b9f323bb",
            "cuda/redist/libcurand/linux-x86_64/libcurand-linux-x86_64-10.3.9.90-archive.tar.xz" to
                "32a5ec30be446c1b7228d1bc502b2f029cc8b59a5e362c70d960754fa646778b",
            "cudnn/redist/cudnn/linux-x86_64/cudnn-linux-x86_64-9.10.2.21_cuda12-archive.tar.xz" to
                "d0defcbc4c6dad711ff4cb66d254036a300c9071b07c7b64199aacab534313c1",
        )
        val complete = cudaLibDir.resolve(".complete")
        onlyIf { !complete.exists() }
        doLast {
            cudaLibDir.mkdirs()
            archives.forEach { (path, sha256) ->
                val name = path.substringAfterLast('/')
                val local = localArchives?.resolve(name)?.takeIf { it.isFile }
                val archive = local ?: temporaryDir.resolve(name)
                if (local == null) {
                    logger.lifecycle("Downloading $name")
                    URI("https://developer.download.nvidia.com/compute/$path").toURL().openStream().use {
                        Files.copy(it, archive.toPath(), StandardCopyOption.REPLACE_EXISTING)
                    }
                }
                val digest = MessageDigest.getInstance("SHA-256")
                archive.inputStream().use { input ->
                    val buffer = ByteArray(1 shl 20)
                    while (true) {
                        val read = input.read(buffer)
                        if (read < 0) break
                        digest.update(buffer, 0, read)
                    }
                }
                val actual = digest.digest().joinToString("") { "%02x".format(it) }
                check(actual == sha256) { "${archive.name}: sha256 $actual, expected $sha256" }
                val tar = ProcessBuilder(
                    "tar", "-xJf", archive.path, "-C", cudaLibDir.path,
                    "--strip-components=2", "--wildcards", "*/lib/*.so*",
                ).inheritIO().start()
                check(tar.waitFor() == 0) { "Could not extract ${archive.name}" }
                if (local == null) archive.delete()
            }
            complete.writeText("")
        }
    }

    // Embeds every indexable line of seforim.db into build/vectors (the private package's EmbedCorpus)
    tasks.register<JavaExec>("embedCorpus") {
        group = "application"
        description = "Embed the corpus of seforim.db into dense vectors for the fused Lucene index (semantic search)."

        classpath = semanticEmbedder
        mainClass.set("io.github.kdroidfilter.seforim.semantic.EmbedCorpusKt")
        args(
            "--db", seforimDb,
            "--out", vectorsDir.path,
            "--gpu", cuda.toString(),
        )
        if (cuda) {
            dependsOn(prepareCudaRuntime)
            val inherited = System.getenv("LD_LIBRARY_PATH")
            environment("LD_LIBRARY_PATH", listOfNotNull(cudaLibDir.path, inherited).joinToString(File.pathSeparator))
        }
        jvmArgs("-Xmx8g", "--enable-native-access=ALL-UNNAMED")
    }
}
