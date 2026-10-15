import java.security.MessageDigest
import java.util.zip.ZipFile
import javax.inject.Inject

// lucene-core for Android (ART, API 35+): the upstream jar with two classes patched, as Lucene 10 assumes a HotSpot JVM
// in them (Runtime.version(), modules). The patched sources are the upstream ones plus a guard, marked "SeforimLibrary".
plugins {
    `java-library`
}

group = "io.github.kdroidfilter.seforimlibrary"

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(libs.versions.jvmToolchain.get().toInt()))
}

tasks.withType<JavaCompile>().configureEach {
    // Lucene's own level
    options.release.set(21)
}

val upstream: Configuration by configurations.creating {
    isTransitive = false
}

dependencies {
    compileOnly(libs.lucene.core)
    upstream(libs.lucene.core)
}

// The upstream classes the patch replaces, by SHA-256: a Lucene bump that changes them fails the build, so the patch
// is diffed against the new sources and redone instead of shipping stale code
val replaced =
    mapOf(
        "org/apache/lucene/internal/vectorization/VectorizationProvider.class" to "317cbaed36506e22e6cfaec16eb2e1612465b55bb36d06125fd5b04e72d9e21c",
        "org/apache/lucene/internal/vectorization/VectorizationProvider\$Holder.class" to "f416745bad90bfde50eaee0e21ec6ec3763e762114914700a8ad0915f54ff023",
        "org/apache/lucene/util/HotspotVMOptions.class" to "beb595fb64a8cc73b602976d77ac2f84b2935b113305117074440ac706a301a3",
    )

val checkUpstream by tasks.registering {
    val jars: FileCollection = files(upstream)
    val expected = replaced
    inputs.files(jars)
    doLast {
        ZipFile(jars.singleFile).use { zip ->
            for ((entry, sha) in expected) {
                val bytes = zip.getInputStream(zip.getEntry(entry) ?: error("$entry is gone from ${jars.singleFile.name}")).readBytes()
                val actual = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
                check(actual == sha) { "$entry changed in ${jars.singleFile.name}: redo the patch of lucene-android on its new sources" }
            }
        }
    }
}

// ArchiveOperations, not the script's zipTree: the configuration cache can't keep references to the script
interface ArchiveAccess {
    @get:Inject
    val archives: ArchiveOperations
}
val archiveAccess = objects.newInstance<ArchiveAccess>()

tasks.jar {
    dependsOn(checkUpstream)
    // The patched classes (compiled here) replace the upstream ones; everything else, services included, is upstream
    val access = archiveAccess
    from(upstream.elements.map { jars -> jars.map { access.archives.zipTree(it.asFile) } }) {
        exclude(replaced.keys)
        exclude("META-INF/MANIFEST.MF")
    }
    duplicatesStrategy = DuplicatesStrategy.FAIL
}

// Consumers get the merged jar only: the secondary variants (classes, resources directories) would hand them the two
// patched classes without the rest of Lucene
listOf("apiElements", "runtimeElements").forEach { name ->
    configurations.named(name) { outgoing.variants.clear() }
}
