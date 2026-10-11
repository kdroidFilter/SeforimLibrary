plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
}

kotlin {
    jvmToolchain(libs.versions.jvmToolchain.get().toInt())

    jvm()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kermit)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmMain.dependencies {
            implementation(libs.sqlite.jdbc)
            implementation(libs.slf4j.simple)
        }

        jvmTest.dependencies {
            implementation(libs.kotlinx.coroutines.test)
        }
    }
}

val defaultDb: String = rootProject.layout.buildDirectory.file("seforim.db").get().asFile.absolutePath
val decisionsFile: String = layout.projectDirectory.file("../data/ref-arbitration.jsonl").asFile.absolutePath

/**
 * Fills the LLM decision cache for the ambiguous citations of the corpus. The only task that calls the network.
 * Usage:
 *   ./gradlew :reflinks:arbitrateRefs -PseforimDb=… -PllmKeyFile=~/key.txt -PmaxSpend=10 [-PmaxCases=5000] [-Pconfirm]
 * Without -Pconfirm it only prints the estimate (cache misses, cost, remaining credit) and exits.
 */
tasks.register<JavaExec>("arbitrateRefs") {
    group = "application"
    description = "Resolve ambiguous citations with the LLM and append the decisions to generator/data/ref-arbitration.jsonl."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.reflinks.ArbitrateRefsKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    jvmArgs = listOf("-Xmx6g", "--enable-native-access=ALL-UNNAMED")
    args = buildList {
        add("--db=" + ((project.findProperty("seforimDb") as String?) ?: defaultDb))
        add("--decisions=$decisionsFile")
        add("--sefaria-schemas=" + rootProject.layout.projectDirectory.dir("generator/sefariasqlite/build/sefaria/export/schemas").asFile.absolutePath)
        (project.findProperty("llmKeyFile") as String?)?.let { add("--key-file=$it") }
        (project.findProperty("llmModel") as String?)?.let { add("--model=$it") }
        (project.findProperty("maxSpend") as String?)?.let { add("--max-spend=$it") }
        (project.findProperty("maxCases") as String?)?.let { add("--max-cases=$it") }
        (project.findProperty("workers") as String?)?.let { add("--workers=$it") }
        (project.findProperty("classes") as String?)?.let { add("--classes=$it") }
        if (project.hasProperty("confirm")) add("--confirm")
        if (project.hasProperty("retryInvalid")) add("--retry-invalid")
    }
}

/**
 * Writes the citation links into the text of the database (no network: deterministic links + cached LLM decisions),
 * in place, or into a copy with -PrefLinksOut. Usage: ./gradlew :reflinks:generateRefLinks [-PseforimDb=…] [-PrefLinksOut=…]
 */
tasks.register<JavaExec>("generateRefLinks") {
    group = "application"
    description = "Write <a class=\"ref\" href=\"seforim://line/<id>\"> links into seforim.db."
    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.reflinks.GenerateRefLinksKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")
    jvmArgs = listOf("-Xmx8g", "--enable-native-access=ALL-UNNAMED")
    args = listOfNotNull(
        "--db=" + ((project.findProperty("seforimDb") as String?) ?: defaultDb),
        (project.findProperty("refLinksOut") as String?)?.let { "--out=$it" },
        "--decisions=$decisionsFile",
        "--sefaria-schemas=" + rootProject.layout.projectDirectory.dir("generator/sefariasqlite/build/sefaria/export/schemas").asFile.absolutePath,
    )
}
