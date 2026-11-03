# The compact seforim.db

The database that ships is not the generator's: `:packaging:compactDb` rewrites it, 2.4x smaller, for the phones
(small storage, little RAM for the page cache, slow flash) as for the desktop.

```bash
./gradlew :packaging:compactDb                      # build/seforim.db → build/seforim.compact.db
./gradlew :packaging:compactDb -PseforimDb=… -PcompactDb=…
```

## What changes

- **Line texts deflated with a dictionary** (`LineTextCodec`, dao). A line is 750 bytes on average: deflate alone
  gets 2.5x on lines that short, a dictionary of the corpus' own text 3.1x (zstd with a trained dictionary 3.2x, for
  a native library on every platform; blocks of 32 lines 3.8x, but every query that reads a text would change). Each
  line still reads on its own, through the same queries: `content` is `BLOB AS String`, and SQLDelight's adapter
  decodes it.
  - A compressed value is `0xFF`, the dictionary's id, then raw deflate; anything else is plain UTF-8 (the generator
    writes plain text, so do older databases, and both read the same way).
  - The dictionary is in the database (`line_codec_dict`) and in the packaging's resources
    (`line_codec_dict/1.bin`): every release compresses with the same one, so deltas carry texts that read with the
    client's. `-PtrainDictionaries=<dir>` builds a new one (a release with it can't be reached by a delta: the
    logical hash covers the dictionaries).
  - JVM and Android use `java.util.zip`, iOS the system's zlib: no native library to ship.
  - `heRef` stays plain: compressing it saves 180 MB but costs a second inflate per line.
- **Indexes**: `idx_line_book_index` covers `charCount` (a book's scrollbar no longer reads all its texts),
  `idx_line_toc` is partial (a tenth of the lines name their TOC entry), and the indexes no query used go
  (`idx_line_heref`, 313 MB; `idx_link_source_line` and `idx_link_type`, prefixes of others; `idx_toc_text`, the
  UNIQUE constraint's twin).
- **Pages**: written once, in order and full (the generator's had 13% free space), 4 KiB (a line read at random costs
  one 4 KiB page, not 16). No `ANALYZE`: the queries are tuned on plans without statistics.
- **SQLite's memory per platform** (`SqliteTuning`): the desktop keeps its 256 MB cache and 512 MB map; Android
  takes a 16 MB cache and no map (mapped pages count in the memory Android kills apps by); iOS 32 MB and 256 MB.
- `PatchDbSchema.CURRENT_VERSION` is 2: a client with a version 1 database takes the full bundle, and an applier of
  version 1 refuses version 2 patches. An app version that predates `LineTextCodec` can't read a compact database.

## Results (the 2026-10 corpus: 7,019,636 lines, 4,428,487 links)

| | generator's | compact | |
|---|---:|---:|---:|
| file | 8,022 MB | 3,382 MB | **2.37x** |
| `line` | 6,184 MiB | 2,210 MiB | 2.8x |
| line indexes | 575 MiB | 224 MiB | 2.6x |
| links and their indexes | 545 MiB | 450 MiB | 1.2x |

Every line reads back byte for byte (6,893,769 compressed, the others too short to gain).

Reads through the repository (`DbReadWorkload`), each phase cold (the file dropped from the page cache) then warm,
with the bytes read from the disk:

| | desktop (JVM, NVMe), cold | read | warm | Android 10 emulator, cold | warm |
|---|---:|---:|---:|---:|---:|
| open 150 books (char counts + 50 lines) | 445 → 210 ms | 226 → 19 MB | 81 → 95 ms | 449 → 301 ms | 217 → 225 ms |
| scroll, 300 pages of 50 lines | 168 → 199 ms | 51 → 22 MB | 55 → 114 ms | 288 → 337 ms | 149 → 245 ms |
| 3,000 lines at random | 467 → 333 ms | 67 → 39 MB | 55 → 54 ms | 704 → 622 ms | 256 → 271 ms |
| commentaries of 300 lines | 49 → 49 ms | 7 → 14 MB | 9 → 7 ms | 123 → 96 ms | 60 → 67 ms |

The compact database reads 2 to 12 times fewer bytes. A line costs about 5 µs more to decode on ART (1.4 µs of it
installing the dictionary), so 0.25 ms more per page of 50 lines read from the cache. Both machines read their disk
at over 700 MB/s. A small phone's flash reads at random at a few tens of MB/s, and its page cache holds a working
set 2.4 times smaller: there the bytes count more than the decoding.

```bash
BENCH_DB=/path/seforim.db ./gradlew :dao:jvmTest --tests '*DbReadBenchmark*' --rerun -i
# Android: see AndroidDbReadBenchmark (adb push, then am instrument per phase)
```
