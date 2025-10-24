
plugins {
    alias(libs.plugins.multiplatform)
    alias(libs.plugins.kotlinx.serialization)
}

kotlin {
    jvmToolchain(21)

    jvm()

    sourceSets {
        commonMain.dependencies {
            api(project(":core"))
            api(project(":dao"))

            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.coroutines.test)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.datetime)
            implementation(libs.kermit)
            implementation("org.jsoup:jsoup:1.17.2")
            implementation("org.slf4j:slf4j-simple:2.0.17")
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }


        jvmMain.dependencies {
            implementation(libs.kotlinx.coroutines.swing)
            implementation(libs.sqlDelight.driver.sqlite)
        }

    }

}


// Run the generator's main (BuildFromScratch.kt) on the JVM target
// Usage (from repo root):
//   SEFORIM_DB=/path/to/seforim.db \
//   OTZARIA_SOURCE_DIR=/path/to/otzaria_source \
//   ./gradlew :generator:runBuildFromScratch --no-daemon
tasks.register<JavaExec>("runBuildFromScratch") {
    group = "application"
    description = "Run the Otzaria DB generator (BuildFromScratch). Requires SEFORIM_DB and OTZARIA_SOURCE_DIR."

    // Ensure JVM classes/jar are built
    dependsOn("jvmJar")

    // Top-level main() in BuildFromScratch.kt compiles to this class name
    mainClass.set("io.github.kdroidfilter.seforimlibrary.generator.BuildFromScratchKt")

    // Use the JVM runtime classpath for the multiplatform 'jvm' target
    // and include the project's jar itself
    classpath = files(tasks.named("jvmJar")) + configurations.getByName("jvmRuntimeClasspath")

    // Give the process a reasonable heap and GC for heavy indexing
    jvmArgs = listOf("-Xmx4g", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=200")
}

