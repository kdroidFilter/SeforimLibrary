@file:OptIn(ExperimentalForeignApi::class)

package io.github.kdroidfilter.seforimlibrary.search.index.bridge

import io.github.kdroidfilter.seforimlibrary.search.jni.JNIEnvVar
import io.github.kdroidfilter.seforimlibrary.search.jni.JNI_CreateJavaVM
import io.github.kdroidfilter.seforimlibrary.search.jni.JNI_EDETACHED
import io.github.kdroidfilter.seforimlibrary.search.jni.JNI_GetCreatedJavaVMs
import io.github.kdroidfilter.seforimlibrary.search.jni.JNI_OK
import io.github.kdroidfilter.seforimlibrary.search.jni.JNI_TRUE
import io.github.kdroidfilter.seforimlibrary.search.jni.JNI_VERSION_21
import io.github.kdroidfilter.seforimlibrary.search.jni.JavaVMInitArgs
import io.github.kdroidfilter.seforimlibrary.search.jni.JavaVMOption
import io.github.kdroidfilter.seforimlibrary.search.jni.JavaVMVar
import io.github.kdroidfilter.seforimlibrary.search.jni.jbyteVar
import io.github.kdroidfilter.seforimlibrary.search.jni.jclass
import io.github.kdroidfilter.seforimlibrary.search.jni.jintVar
import io.github.kdroidfilter.seforimlibrary.search.jni.jmethodID
import io.github.kdroidfilter.seforimlibrary.search.jni.jvalue
import kotlinx.atomicfu.locks.SynchronizedObject
import kotlinx.atomicfu.locks.synchronized
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.cstr
import kotlinx.cinterop.get
import kotlinx.cinterop.invoke
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value

/**
 * The Java runtime the iOS app embeds: OpenJDK's mobile port (Zero, the interpreter, as iOS forbids generated code),
 * linked statically into the app. Lucene runs there; the app calls it through [call].
 *
 * The runtime reads its classes from `<app executable dir>/lib` (its `java.home`, set by the VM itself); [classPath]
 * names the jars of the search (see the `iosJvmClasspath` task of SeforimLibrary's `search` module).
 */
class EmbeddedJvm private constructor(
    private val vm: CPointer<JavaVMVar>,
) {
    private val lock = SynchronizedObject()

    // One bridge method per process (IndexBridgeServer.call)
    private var bridge: Pair<jclass, jmethodID>? = null

    /**
     * Calls the static method `byte[] method(byte[])` of [className] with [request], from the calling thread
     * (attached to the runtime on its first call), and returns its result; throws when it threw.
     */
    fun call(
        className: String,
        method: String,
        request: ByteArray,
    ): ByteArray {
        val env = attachedEnv()
        val functions = env.pointed.pointed!!
        val (clazz, methodId) = bridgeMethod(env, className, method)
        val array = functions.NewByteArray!!(env, request.size) ?: throwPending(env, "NewByteArray")
        try {
            if (request.isNotEmpty()) {
                request.usePinned { pinned ->
                    functions.SetByteArrayRegion!!(env, array, 0, request.size, pinned.addressOf(0).reinterpret<jbyteVar>())
                }
            }
            val result =
                memScoped {
                    val args = allocArray<jvalue>(1)
                    args[0].l = array
                    functions.CallStaticObjectMethodA!!(env, clazz, methodId, args)
                }
            if (functions.ExceptionCheck!!(env).toInt() != 0) throwPending(env, "$className.$method")
            result ?: error("$className.$method returned null")
            try {
                val size = functions.GetArrayLength!!(env, result)
                val bytes = ByteArray(size)
                if (size > 0) {
                    bytes.usePinned { pinned ->
                        functions.GetByteArrayRegion!!(env, result, 0, size, pinned.addressOf(0).reinterpret<jbyteVar>())
                    }
                }
                return bytes
            } finally {
                functions.DeleteLocalRef!!(env, result)
            }
        } finally {
            functions.DeleteLocalRef!!(env, array)
        }
    }

    // The class and method are resolved once: a global reference keeps the class
    private fun bridgeMethod(
        env: CPointer<JNIEnvVar>,
        className: String,
        method: String,
    ): Pair<jclass, jmethodID> =
        synchronized(lock) {
            bridge ?: memScoped {
                val functions = env.pointed.pointed!!
                val local = functions.FindClass!!(env, className.replace('.', '/').cstr.getPointer(this)) ?: throwPending(env, className)
                val global = functions.NewGlobalRef!!(env, local)!!
                functions.DeleteLocalRef!!(env, local)
                val methodId = functions.GetStaticMethodID!!(env, global, method.cstr.getPointer(this), "([B)[B".cstr.getPointer(this))
                    ?: throwPending(env, "$className.$method")
                (global to methodId).also { bridge = it }
            }
        }

    private fun attachedEnv(): CPointer<JNIEnvVar> =
        memScoped {
            val env = alloc<CPointerVar<JNIEnvVar>>()
            val invoke = vm.pointed.pointed!!
            val status = invoke.GetEnv!!(vm, env.ptr.reinterpret(), JNI_VERSION_21)
            if (status == JNI_EDETACHED) {
                // Daemon: an attached Kotlin worker never holds the runtime up at exit
                check(invoke.AttachCurrentThreadAsDaemon!!(vm, env.ptr.reinterpret(), null) == JNI_OK) { "Can't attach the thread to the Java runtime" }
            } else {
                check(status == JNI_OK) { "GetEnv failed: $status" }
            }
            env.value!!
        }

    private fun throwPending(
        env: CPointer<JNIEnvVar>,
        what: String,
    ): Nothing {
        val functions = env.pointed.pointed!!
        if (functions.ExceptionCheck!!(env).toInt() != 0) {
            functions.ExceptionDescribe!!(env)
            functions.ExceptionClear!!(env)
        }
        error("Java call failed: $what")
    }

    companion object {
        private val lock = SynchronizedObject()
        private var instance: EmbeddedJvm? = null

        /**
         * The Java runtime of this process, started on the first call with [classPath] and [options] (`-Xmx...`,
         * `-D...`); a process has one, so later calls get it as it is.
         */
        fun start(
            classPath: List<String>,
            options: List<String> = emptyList(),
        ): EmbeddedJvm =
            synchronized(lock) {
                instance ?: (existing() ?: create(classPath, options)).also { instance = it }
            }

        private fun existing(): EmbeddedJvm? =
            memScoped {
                val vms = allocArray<CPointerVar<JavaVMVar>>(1)
                val count = alloc<jintVar>()
                if (JNI_GetCreatedJavaVMs(vms, 1, count.ptr) == JNI_OK && count.value > 0) vms[0]?.let { EmbeddedJvm(it) } else null
            }

        private fun create(
            classPath: List<String>,
            options: List<String>,
        ): EmbeddedJvm =
            memScoped {
                val all = listOf("-Djava.class.path=" + classPath.joinToString(":")) + options
                val vmOptions = allocArray<JavaVMOption>(all.size)
                all.forEachIndexed { i, option -> vmOptions[i].optionString = option.cstr.getPointer(this) }
                val args = alloc<JavaVMInitArgs>()
                args.version = JNI_VERSION_21
                args.nOptions = all.size
                args.options = vmOptions
                args.ignoreUnrecognized = JNI_TRUE.convert()
                val vm = alloc<CPointerVar<JavaVMVar>>()
                val env = alloc<CPointerVar<JNIEnvVar>>()
                val status = JNI_CreateJavaVM(vm.ptr, env.ptr.reinterpret(), args.ptr)
                check(status == JNI_OK) { "JNI_CreateJavaVM failed: $status" }
                EmbeddedJvm(vm.value!!)
            }
    }
}
