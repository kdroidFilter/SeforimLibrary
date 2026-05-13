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

tasks.register<JavaExec>("producePatchAndVerify") {
    group = "application"
    description = "Produce patch.db from (prevDb, newDb) and verify apply reproduces newDb's logical hash."

    dependsOn("jvmJar")
    mainClass.set("io.github.kdroidfilter.seforimlibrary.common.patch.PatchPipelineCliKt")
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    val prev = project.findProperty("prevDb") as String?
    val new = project.findProperty("newDb") as String?
    val out = project.findProperty("out") as String?
        ?: rootProject.layout.buildDirectory.file("patch.db").get().asFile.absolutePath
    if (prev != null) systemProperty("prevDb", prev)
    if (new != null) systemProperty("newDb", new)
    systemProperty("out", out)
    project.findProperty("fromVersion")?.let { systemProperty("fromVersion", it as String) }
    project.findProperty("toVersion")?.let { systemProperty("toVersion", it as String) }
    // Optional release-meta knobs.
    listOf(
        "releaseMeta", "fullBundleUrl", "fullBundleSha", "fullBundleSize",
        "manifestBaseUrl", "fromSchemaVersion", "toSchemaVersion",
    ).forEach { key ->
        project.findProperty(key)?.let { systemProperty(key, it as String) }
    }

    jvmArgs = listOf("-Xmx10g", "-XX:+UseG1GC")
}
