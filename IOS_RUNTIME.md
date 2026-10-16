# The search on iOS: Lucene in an embedded Java runtime

Lucene needs a JVM. On iOS, the app embeds one: OpenJDK's mobile port ([openjdk/mobile](https://github.com/openjdk/mobile)),
built as static libraries with the Zero VM (an interpreter, as iOS forbids generated code). Only the Lucene part of the
search runs there; everything else is Kotlin/Native.

## Architecture

```
iOS app (Kotlin/Native)                                 Java runtime in the same process (OpenJDK Zero)
───────────────────────                                 ─────────────────────────────────────────────────
LexicalSearchEngine, MagicDictionaryIndex (sqlite3),
snippets, highlights, hybrid fusion, semantic model
(ONNX Runtime C API), view models...
        │ LuceneIndex
        ▼
JvmLuceneIndex ── CBOR over JNI (EmbeddedJvm.call) ──▶ IndexBridgeServer ──▶ LuceneIndexBackend (Lucene 10)
```

- `search/src/commonMain/.../index/LuceneIndex.kt`: the only part of the search that runs Lucene: analysis, searches
  (`IndexQuery`, plain data), cursors, facets, KNN. Lucene runs on a JVM only (ART can't keep up with the Java versions
  Lucene requires: Java 21 for 10, 25 for 11): the desktop implements it in process (`LuceneIndexBackend`), the mobile
  apps through the bridge (`BridgedLuceneIndex`), with the same implementation running in the Java runtime they embed:
  in the app on iOS (JNI), in a process of the app's own on Android.
- `EmbeddedJvm` starts the runtime (`JNI_CreateJavaVM`), attaches the calling threads, calls
  `IndexBridgeServer.call(byte[]): byte[]` (CBOR of `BridgeRequest` / `BridgeResponse`).
- Lucene runs unpatched there (Zero is HotSpot), with `NIOFSDirectory` (no Panama mmap on Zero).

Verified in the simulator on the real index (7M documents): the same facets, snippets, highlights, title suggestions
and find-in-book candidates as the desktop, and the same hits but for Lucene's score noise (see `SearchDump`); the whole
app search (Zayit's `searchkit`, hybrid with the semantic search) gives the same dump as the desktop.

## The runtime

The runtimes are built by the CI of [kdroidFilter/openjdk-mobile](https://github.com/kdroidFilter/openjdk-mobile)
(branch `mobile-runtime`: the recipe, `openjdk-mobile.patch` and `build.sh`): a stable OpenJDK, 27 (`openjdk/jdk27u`'s GA
releases), with openjdk/mobile's port (which follows the JDK's mainline) ported to it. Every day, a new GA or recipe gets
its release, named like the JBR's, `jdk-mobile-<version>b<build>.<revision>` (`jdk-mobile-27b35.2`: `jdk-27-ga`, the
JDK's build 35, the second release there). The build downloads one of them and checks its SHA-256 (`search`'s `javaRuntime<Target>` tasks:
`jdkMobileVersion`, `jdkMobileBuild`, `jdkMobileRevision`, `mobileRuntimeSha256`, extracted in
`search/build/javaRuntime/<target>`; the iOS bindings only need its `include/`, extracted per target and release in
`search/build/jni`); moving to a newer runtime is changing those, then running the tests below.

Besides openjdk/mobile's port, the patch carries two fixes for Zero on iOS:

1. `os_bsd_zero.cpp`: the W^X state (`DefaultWXWriteMode`, `os::_jit_exec_enabled`, `Thread::wx_enable_write`...) the
   shared code needs on Apple aarch64 is only defined for the C1/C2 port (`bsd_aarch64`): Zero gets its own, without page
   protection switches (it generates no code).
2. `os_bsd.cpp`: `pd_commit_memory` asks for executable pages (the code cache), which iOS refuses without the JIT
   entitlement; Zero never executes them, so they are committed read-write.

The assets (`jdk-mobile-27-ios-simulator-aarch64-b35.2.tar.gz`, `…-ios-device-…`) hold `static/` (`libjvm.a`, the JDK's
native libraries, `libffi.a`), `include/` (`jni.h`, `jni_md.h`: the iOS bindings' headers) and `lib/` (the runtime's
`java.home`: `conf/`, `lib/`, `modules/<module>/...`). `make images` and `make jmods` don't work for iOS (they link a
dynamic libjvm): the runtime boots from an exploded image instead of a `lib/modules` jimage.

## Packaging it in the app

**Linking** (the app target, or a test binary):

```
-force_load static/libjvm.a
-force_load static/{libjava,libnio,libnet,libzip,libjimage,libverify,libextnet,libfallbackLinker,libsyslookup}.a
static/libffi.a -export_dynamic -lz -lc++ -lsqlite3
-framework Foundation -framework CoreFoundation -framework Security -framework SystemConfiguration
```

`-force_load` and `-export_dynamic` are required: the runtime finds its natives by name (`dlsym`: `JIMAGE_Open`,
`Java_java_lang_...`, `JNI_OnLoad_*`), which the linker would otherwise drop or hide. The semantic search adds ONNX
Runtime's `onnxruntime.xcframework` (`onnxruntime-c` 1.20.0, the version of the desktop and Android builds).

**Files** — the VM sets its `java.home` to `<executable dir>/lib` itself (`-Djava.home` is ignored), so the app bundle
holds:

```
App.app/lib/{conf,lib,modules}/...   (the runtime's lib/: java.base, java.logging, java.management, jdk.unsupported)
App.app/lib/app/*.jar                (./gradlew :search:mobileJvmClasspath: search-jvm, lucene-core, kotlin-stdlib, kotlinx-serialization)
```

About 50 MB uncompressed. Zayit's `./gradlew :searchkit:assembleIosJavaRuntimeDevice` assembles `static/` and `lib/`
(with the jars); `searchkit`'s `IosJavaRuntime` finds the jars in `lib/app` by default.

## Tests

```bash
./gradlew :search:iosSimulatorArm64Test -PindexDir=<seforim.db.lucene> -PluceneQueryCache=off
./gradlew :search:jvmTest --tests '*RealIndexParityTest*' -PindexDir=<seforim.db.lucene> -PluceneQueryCache=off
```

They write `search/build/parity/ios.txt` and `jvm.txt` (`SearchDump`). `-PluceneQueryCache=off` turns off Lucene's
query cache: it changes the last digits of the scores from one search to the next. Two runs on the same platform differ
anyway by Lucene's process-dependent clause order (`BooleanQuery` rewrites through a `HashMap` keyed by class identity
hashes), so the comparison allows score differences: facets, snippets and highlights must be identical.

## Android

Android 10+ (API 29: HotSpot's aarch64 code needs native ELF TLS). ART and HotSpot can't share a process (both define
`JNI_CreateJavaVM`, both handle `SIGSEGV`), so Lucene runs in a Java process of the app's own, the server VM (with its
JIT, which Android allows) of the same release's Android runtimes (`jdk-mobile-27-android-aarch64-b35.2.tar.gz` and
`…-android-x64-…`: devices and x86_64 emulators, the CI smoke-testing the latter on one), behind
the same bridge as iOS, its calls carried on the process's standard input and output (`JavaProcessTransport`,
`IndexBridgeServer.main`) rather than JNI.

- A runtime holds the launcher, `jniLibs/<abi>/libjvmlauncher.so`: an executable with the VM and the JDK's
  natives in, packaged as a library (Android only executes native code from the APK; the app sets
  `packaging.jniLibs.useLegacyPackaging = true` so it is installed as a file), and `lib/`, its `java.home` (one per
  architecture: some of its generated classes, `UnixConstants` among them, depend on it). The
  launcher runs `IndexBridgeServer.main`; everything the VM prints goes to standard error, standard output carries the
  answers.
- The process starts on the first call and again after it ends (Android may kill an app's child processes in the
  background): the calls sent to a process that ends fail, the next ones start another.
- `JavaProcessTransportTest` runs the same transport on the desktop, a `java` standing for the launcher. On a device,
  Zayit's `searchkit-check` app runs the whole search.
