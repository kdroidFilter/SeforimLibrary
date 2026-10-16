package io.github.kdroidfilter.seforimlibrary.search.index.bridge

import co.touchlab.kermit.Logger
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.io.IOException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicInteger

private const val SERVER_CLASS = "io.github.kdroidfilter.seforimlibrary.search.index.bridge.IndexBridgeServer"
private const val STDERR_LINES_KEPT = 40

// Far above any answer (a page of hits, facets), far below a misread frame
private const val MAX_ANSWER_BYTES = 64 * 1024 * 1024

/**
 * The calls to a Java process of the app's own, for Android, whose ART can't run Lucene: OpenJDK's mobile port (built
 * for Android, see IOS_RUNTIME.md) runs `IndexBridgeServer.main`, which serves the calls on its standard input and
 * output. A separate process, as ART and HotSpot can't share one. It starts on the first call and again after it dies
 * (Android may kill an app's child processes in the background).
 *
 * Android only executes the native code an APK holds: [launcher] is the runtime's launcher, an executable packaged as a
 * library (`libjvmlauncher.so`, from the app's native library dir), the VM linked in; the JDK's classes and data are in
 * the app's files ([javaHome], an exploded image: `modules/`, `conf/`, `lib/`).
 *
 * @param classPath the jars of the search's Java side (`mobileJvmClasspath`, the same as iOS)
 * @param options the Java process's options (`-Xmx...`)
 */
class JavaProcessTransport(
    private val launcher: File,
    private val javaHome: File,
    private val classPath: List<File>,
    private val options: List<String> = listOf("-Xmx512m"),
) : BridgeTransport,
    AutoCloseable {
    private val lock = Any()
    private var running: Running? = null
    private val ids = AtomicInteger()
    private val logger = Logger.withTag("SeforimJava")

    // A started process with the calls sent to it: when it ends, they fail, and those of the next process don't
    private class Running(
        val process: Process,
    ) {
        val input = DataOutputStream(BufferedOutputStream(process.outputStream))
        val pending = ConcurrentHashMap<Int, CompletableFuture<ByteArray>>()
        val stderr = ArrayDeque<String>()
    }

    override fun call(request: ByteArray): ByteArray {
        val id = ids.incrementAndGet()
        val answer = CompletableFuture<ByteArray>()
        synchronized(lock) {
            val run = started()
            run.pending[id] = answer
            try {
                run.input.writeInt(id)
                run.input.writeInt(request.size)
                run.input.write(request)
                run.input.flush()
            } catch (e: IOException) {
                // The process is gone: its reader fails the calls sent to it, this one included
                run.pending.remove(id)
                throw IllegalStateException("The Java process can't be reached", e)
            }
        }
        return try {
            answer.get()
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }
    }

    // The running process, starting it when there is none
    private fun started(): Running {
        running?.let { return it }
        val command = listOf(launcher.path, javaHome.path, classPath.joinToString(":") { it.path }, SERVER_CLASS) + options
        val run = Running(ProcessBuilder(command).start())
        running = run
        Thread({ readAnswers(run) }, "seforim-java-answers").apply { isDaemon = true }.start()
        Thread({ readErrors(run) }, "seforim-java-stderr").apply { isDaemon = true }.start()
        return run
    }

    private fun readAnswers(run: Running) {
        val answers = DataInputStream(BufferedInputStream(run.process.inputStream))
        try {
            while (true) {
                val id = answers.readInt()
                val size = answers.readInt()
                // Anything but a frame here means the process wrote something else on its output: stop reading it
                check(size in 0..MAX_ANSWER_BYTES) { "Not an answer from the Java process (size $size)" }
                val bytes = ByteArray(size).also { answers.readFully(it) }
                run.pending.remove(id)?.complete(bytes)
            }
        } catch (failure: Throwable) {
            logger.w { "Java process answers: $failure" }
            run.process.destroy()
            // The process ended: the calls sent to it fail, the next one starts another
            val exit = runCatching { run.process.waitFor() }.getOrNull()
            val reason = IllegalStateException("The Java process ended (exit $exit): ${synchronized(run.stderr) { run.stderr.joinToString("\n") }}")
            synchronized(lock) {
                if (running === run) running = null
            }
            run.pending.keys.toList().forEach { id -> run.pending.remove(id)?.completeExceptionally(reason) }
        }
    }

    private fun readErrors(run: Running) {
        run.process.errorStream.bufferedReader().forEachLine { line ->
            logger.w { line }
            synchronized(run.stderr) {
                run.stderr.addLast(line)
                if (run.stderr.size > STDERR_LINES_KEPT) run.stderr.removeFirst()
            }
        }
    }

    override fun close() {
        synchronized(lock) {
            running?.let { run ->
                runCatching { run.input.close() }
                run.process.destroy()
            }
            running = null
        }
    }
}
