# JSON Schema Guide for This Corpus

This repository is a corpus of Jewish texts. Each work lives in a subject-specific directory and is represented by a `merged.json` file. This guide explains the JSON structure so that a language model can reliably interpret and navigate the data, including less obvious edge cases observed across many files.

## 1. Top-Level Keys in `merged.json`

Most files share a common set of top-level keys:

- `title` (string): Work title in English, e.g. `"Arukh HaShulchan"`.
- `heTitle` (string): Work title in Hebrew.
- `language` (string): Language code of the text, typically `"he"`.
- `categories` (array of string): High-level classification, e.g. `["Halakhah"]`, `["Kabbalah"]`.
- `text` (array or object): The actual content, stored as nested arrays or an object of sections.
- `schema` (object, optional): Tree structure for complex works with named sections.
- `sectionNames` (array of string, optional): Names of sequential section levels for simpler works.
- `versionTitle` (string): Human-readable name of the primary version.
- `versionSource` (string / URL): Source of the primary version.
- `versions` (array): Each element is `[versionTitle, versionSource]` for known editions.

Observed design pattern:
- Files use either `schema` or `sectionNames`, not both (in sampled data).
- Simple structure: `sectionNames` + `text` as nested arrays.
- Complex structure: `schema` + `text` as an object (possibly nested objects) of named sections.

Content strings frequently contain Hebrew text, occasional HTML markup (e.g. `<b>...</b>`, `<br>`), and embedded newlines.

## 2. Structural Pattern Overview

Across many `merged.json` files, the following high-level patterns recur:

| Pattern ID | Description                                            | `schema` | `sectionNames` | `text` container                         | Typical depth of `text`           | Example file (relative path)                                                           |
|-----------:|--------------------------------------------------------|----------|----------------|------------------------------------------|-----------------------------------|----------------------------------------------------------------------------------------|
| P1         | Single-level sequential sections                       | no       | yes            | array                                    | 1D: `text[i]` is a string        | `Reference/Grammar/Sefat Yeter/merged.json`                                            |
| P2         | Two-level sequential sections (chapter + unit)        | no       | yes            | array                                    | 2D: `text[i][j]` is a string     | `Mishnah/Seder Nezikin/Pirkei Avot/merged.json`; `Kabbalah/Sefer HaBahir/merged.json`* |
| P3         | Two-level sequential sections (chapter + verse)       | no       | yes            | array                                    | 2D: `text[i][j]` is a string     | `Tanakh/Targum/Aramaic Targum/Writings/Aramaic Targum to Psalms/merged.json`          |
| P4         | Flat named sections                                   | yes      | no             | object: `text[sectionName]`             | Usually 1D arrays of strings     | `Liturgy/Other Liturgy Works/Birkat Hamazon/merged.json`                              |
| P5         | Named sections with internal numeric structure        | yes      | no             | object: `text[sectionName]`             | 2D: `text[k][i][j]` is a string  | `Halakhah/Arukh HaShulchan/merged.json`                                                |
| P6         | Multi-axis structure (volume → sub-area → segments)   | yes      | no             | object → object → nested arrays         | Up to 3 levels before strings    | `Responsa/Yein HaTov/merged.json`                                                      |
| P7         | Dictionary-style tree (corpus → book → list)          | yes      | no             | object → object → arrays                | 1D arrays at leaves              | `Reference/Dictionary/Otzar La'azei Rashi/merged.json`                                 |
| P8         | Commentary with intro + default unnamed main section  | yes      | no             | object: keys like `""`, `"Introduction"`| Mixed: arrays, nested arrays     | `Talmud/Yerushalmi/Modern Commentary.../Ohr LaYesharim on Jerusalem Talmud Megillah`   |
| P9         | Multi-part works (parts → subsections → nested arrays)| yes      | no             | object → object → nested arrays         | Often 2D arrays at leaves        | `Chasidut/Chabad/Tanya/merged.json`                                                   |

\*Note: `Sefer HaBahir` uses `sectionNames: ["Siman"]` and a 1D array, but conceptually behaves like P2 with one logical level.

## 2. Simple Model: `sectionNames` + Nested Arrays

In the simple model, `text` is a nested array whose depth equals the length of `sectionNames`.

Examples:
- Mishnah (`Mishnah/Seder Nezikin/Pirkei Avot/merged.json`):
  - `sectionNames: ["Chapter", "Mishnah"]`
  - `text[chapterIndex][mishnahIndex]` is a string (one mishnah).
- Sefer with a single level (`Kabbalah/Sefer HaBahir/merged.json`):
  - `sectionNames: ["Siman"]`
  - `text[simanIndex]` is a string.
- Grammar work (`Reference/Grammar/Sefat Yeter/merged.json`):
  - `sectionNames: ["Paragraph"]`
  - `text[paragraphIndex]` is a string.
- Targum example (`Tanakh/Targum/Aramaic Targum/Writings/Aramaic Targum to Psalms/merged.json`):
  - `sectionNames: ["Chapter", "Verse"]`
  - `text[chapterIndex][verseIndex]` is a string.

Interpretation rules:
- The first index corresponds to `sectionNames[0]`, the second to `sectionNames[1]`, etc.
- Indexing is zero-based; logical numbering is usually index + 1.

## 3. Complex Model: `schema` + Section-Keyed `text`

In the complex model, `schema` describes a tree of named sections and `text` is an object keyed by section names (English titles).

### 3.1 Core `schema` / `text` relationship

Example A (`Halakhah/Arukh HaShulchan/merged.json`):
- `schema`:
  - `heTitle`, `enTitle`, `key` describe the whole work.
  - `nodes` is an array of top-level sections, each with `heTitle`, `enTitle`, and optionally nested `nodes`.
- `text`:
  - Keys correspond to `schema` node `enTitle` values, e.g. `"Orach Chaim"`, `"Introduction"`.
  - `text["Orach Chaim"]` is typically a nested array, such as `[ [ "paragraph1", ... ], ... ]`, where indices represent internal units (e.g. simanim and se'ifim).

Example B (`Liturgy/Other Liturgy Works/Birkat Hamazon/merged.json`):
- `schema.nodes` lists liturgical sections such as `"Blessing on the Food"`.
- `text["Blessing on the Food"]` is an array of strings, each a paragraph or line.

Interpretation rules:
- Use `schema` to discover valid section names and their hierarchy.
- Use keys of `text` to select a section, then treat the value as nested arrays of strings whose depth reflects the logical segmentation of that section.

### 3.2 Multi-axis and nested object structures

Some works introduce additional object layers inside `text`:

- Responsa example (`Responsa/Yein HaTov/merged.json`):
  - Top-level `text` keys: `"Approbations"`, `"Index"`, `"Introduction"`, `"Part I"`, `"Part II"`.
  - Under `"Part I"`, `text["Part I"]` is an object keyed by halakhic sections such as `"Orach Chayim"`, `"Yoreh De'ah"`.
  - Each of these is a nested array, e.g. `text["Part I"]["Orach Chayim"][i][j]` is a string.

- Multi-part sefer (`Chasidut/Chabad/Tanya/merged.json`):
  - Top-level `text` keys: `"Part I; Likkutei Amarim"`, `"Part II; Shaar HaYichud VehaEmunah"`, etc.
  - Each part is an object whose keys match `schema.nodes` (e.g. `"Title Page"`, `"Approbation"`, `"Compiler's Foreword"`, or `""` for the main body).
  - Leaves are typically 2D arrays: `text[part][subsection][i][j]` is a string.

- Calendar-style work (`Second Temple/Megillat Ta'anit/merged.json`):
  - `schema.nodes` lists Hebrew months (`"Nisan"`, `"Iyar"`, ..., `"Adar"`).
  - `text` is an object keyed by month name, e.g. `text["Nisan"]`.
  - Each value is an array of strings (entries for that month).

- Dictionary-like reference (`Reference/Dictionary/Otzar La'azei Rashi/merged.json`):
  - `schema.nodes` first distinguishes `"Talmud"` vs `"Tanakh"`, then specific tractates/books.
  - `text` mirrors this: `text["Talmud"]["Berakhot"]` is an array of strings (entries for that tractate).

### 3.3 Commentary with default unnamed section

Commentary works can reserve an empty string key for the main commentary:

- Example (`Talmud/Yerushalmi/Modern Commentary on Talmud/Ohr LaYesharim on Jerusalem Talmud/Ohr LaYesharim on Jerusalem Talmud Megillah/merged.json`):
  - `schema.nodes` includes `"Introduction"` and an unnamed node (`""`).
  - `text["Introduction"]` is an array of strings (intro paragraphs).
  - `text[""]` is a 2D array; `text[""][i][j]` is a string representing commentary aligned to base text units.

LLMs should treat `""` as “default” or “main body” rather than a literal visible section name.

## 4. Version Metadata

`versionTitle`, `versionSource`, and `versions` describe textual sources:
- `versionTitle`: Preferred label for the primary version (often an edition name).
- `versionSource`: URL or source reference for that version.
- `versions`: An array of `[title, source]` pairs listing known editions or online sources.

LLMs should treat this metadata as provenance information and may use it to attribute or compare texts, but not as part of the user-visible main content.

## 5. Recommended LLM Usage Patterns

When working over this corpus:

- Always inspect `schema` and/or `sectionNames` first to determine how to navigate `text`.
- For a given work, programmatically probe `text` to discover its depth (e.g. array of strings vs. array of arrays of strings) before assuming a fixed shape.
- Treat strings in `text` as canonical content; normalization (e.g. trimming trailing whitespace or HTML tags when needed) is acceptable, but reordering or regrouping segments may break references.
- When referencing passages, prefer coordinates derived from the structure (section name + indices) rather than brittle character offsets.
- Do not assume one uniform structure across all works; instead, infer the model (simple vs. complex) per file using the rules above.

Recommended algorithm sketch for an LLM or tool:
- Check for `schema`:
  - If present, traverse `schema.nodes` to list named sections and possible nested nodes.
  - For each leaf node, locate the corresponding key path in `text` (using `enTitle` values).
- Otherwise, use `sectionNames`:
  - Let `d = len(sectionNames)` be the expected depth of the `text` array.
  - Verify the actual depth by sampling elements (e.g. `text[0]`, `text[0][0]`, etc.).
- In either case, once the container type and depth are known, treat each leaf string as an atomic segment, and build references of the form:
  - Simple: `(sectionNames[0]=i+1, sectionNames[1]=j+1, ...)`.
  - Complex: `(topSectionName, optionalSubsectionName, i, j, ...)`.

