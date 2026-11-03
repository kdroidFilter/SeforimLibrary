# The compact seforim.db

The database that ships is not the generator's: `:packaging:compactDb` rewrites it, 2.5x smaller and faster to read,
for the phones (small storage, little RAM for the page cache, slow flash) as for the desktop.

```bash
./gradlew :packaging:compactDb                      # build/seforim.db → build/seforim.compact.db (about 70 s)
./gradlew :packaging:compactDb -PseforimDb=… -PcompactDb=…
```

## What changes

- **Line texts compressed** (`LineTextCodec`, dao, common code): one byte per Hebrew letter, nikud or taam (UTF-8
  takes two), then LZ77 against 60 KiB of the corpus' own text, LZ4's block format. 3.1x on lines of 750 bytes on
  average, as much as deflate with a dictionary, but decoding is copies only: no entropy decoding, nothing to set up
  per line, no native call, the same Kotlin on the JVM, Android and iOS. Deflate cost 5 µs a line on ART (JNI, the
  dictionary's install, the inflate, the UTF-8), more than plain text's reading saved; this reads faster than plain
  UTF-8 (2 to 3 times fewer bytes from SQLite, chars straight from a table). Each line still reads on its own,
  through the same queries: `content` is `BLOB AS String`, and SQLDelight's adapter decodes it.
  - A compressed value is `0xFF`, the dictionary's id, the length in Hebrew bytes, then the LZ sequences; anything
    else is plain UTF-8 (the generator writes plain text, so do older databases, and both read the same way).
  - The dictionary is in the database (`line_codec_dict`) and in the packaging's resources (`line_codec_dict/1.bin`):
    every release compresses with the same one, so deltas carry texts that read with the client's.
    `-PtrainDictionaries=<dir>` builds a new one (a release with it can't be reached by a delta: the logical hash
    covers the dictionaries).
  - `heRef` stays plain TEXT: short (36 bytes), compressing it would save 180 MB for a second decode per line.
- **Indexes**: `idx_line_book_index` covers `charCount` (a book's scrollbar no longer reads all its texts),
  `idx_line_toc` is partial (a tenth of the lines name their TOC entry), and the indexes no query used go
  (`idx_line_heref`, 313 MB; `idx_link_source_line` and `idx_link_type`, prefixes of others; `idx_toc_text`, the
  UNIQUE constraint's twin).
- **Pages**: written once, in order and full (the generator's had 13% free space), 16 KiB: on flash a 16 KiB read
  costs about a 4 KiB one's latency and holds 50 compressed lines, a page of the reader (4 KiB pages read 3 times
  more often below). No `ANALYZE`: the queries are tuned on plans without statistics.
- **SQLite's memory per platform** (`SqliteTuning`): the desktop keeps its 256 MB cache and 512 MB map; Android
  takes a 16 MB cache and no map (mapped pages count in the memory Android kills apps by); iOS 32 MB and 256 MB.
- `PatchDbSchema.CURRENT_VERSION` is 2: a client with a version 1 database takes the full bundle, and an applier of
  version 1 refuses version 2 patches. **An app version that predates `LineTextCodec` can't read a compact
  database**: the release that ships it needs a minimum app version.

## Results (the 2026-10 corpus: 7,019,636 lines, 4,428,487 links)

| | generator's | compact | |
|---|---:|---:|---:|
| file | 8,022 MB | 3,253 MB | **2.47x** |
| `line` | 6,184 MiB | 2,088 MiB | 3.0x |
| line indexes | 575 MiB | 223 MiB | 2.6x |
| links and their indexes | 545 MiB | 448 MiB | 1.2x |

Every line reads back byte for byte (`CompactDbVerification`, and an independent decoder on a sample). A delta
between two compact releases (20 lines changed) carries those 20 lines, 150 KB, and its apply reproduces the new
release's hash (`producePatchAndVerify`).

Reads through the repository (`DbReadWorkload`), each phase cold (the file dropped from the page cache) then warm.
Cold / warm, in ms:

| | Android 10 (emulator) | small phone (simulated) | desktop (JVM, NVMe) |
|---|---:|---:|---:|
| open 150 books (char counts + 50 lines) | 407–480 / 221–262 → 213–223 / 151–171 | 5,025 / 4,678 → 473 / 58 | 431 / 79 → 177 / 67 |
| jump to 300 pages of random books | 260–268 / 142–144 → 199–204 / 124–131 | 814 / 102 → 476 / 130 | 168 / 59 → 144 / 57 |
| scroll 20 books, 30 pages each | 296–301 / 250–255 → 271–283 / 218–229 | 824 / 140 → 340 / 78 | 122 / 73 → 116 / 87 |
| 3,000 lines at random | 580–626 / 240–252 → 512–517 / 249–258 | 1,559 / 78 → 1,463 / 123 | 380 / 50 → 349 / 54 |
| commentaries of 300 lines | 124–178 / 58–60 → 85–87 / 54 | 200 / 9 → 152 / 9 | 39 / 5 → 45 / 8 |

- **Android 10**: SQLDelight's Android driver on ART, in an x86_64 emulator (2 GB) reading the host's NVMe, two
  rounds each: faster cold everywhere, warm as fast or faster.
- **Small phone**: the JVM with Android's SQLite memory over a simulated storage (`LD_PRELOAD`: a 128 MB page
  cache, each miss 0.2 ms plus 150 MB/s, an entry-level eMMC): the old database's working set no longer fits.
- **Desktop**: faster cold but for the commentaries (mmap reads ahead), warm within 15 ms.
- Slower: warm random lines on the small phone (78 → 123 ms, the old pages fit its cache there), the desktop's
  commentaries (+6 ms cold) and warm scrolling (+14 ms for 1,500 pages).

```bash
BENCH_DB=/path/seforim.db ./gradlew :dao:jvmTest --tests '*DbReadBenchmark*' --rerun -i   # BENCH_PROFILE=android
# Android: see AndroidDbReadBenchmark (adb push, then am instrument per phase)
SOURCE_DB=build/seforim.db COMPACT_DB=build/seforim.compact.db ./gradlew :packaging:jvmTest --tests '*CompactDbVerification*' --rerun
```
