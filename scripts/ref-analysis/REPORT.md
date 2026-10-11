# Analyse des hyperliens de citations (issue #111)

Corpus : `build/seforim.db` (2026-10-09), 7 114 183 lignes, 7 926 livres. Ouverture en lecture seule (`mode=ro`), aucune écriture.
Les scripts sont dans ce dossier. Les données intermédiaires sont dans `/tmp/refan/cands.db`.

## 1. Inventaire

| Source | Volume | Remarque |
|---|---|---|
| Groupes `( … )` / `[ … ]` contenant de l'hébreu | 5 455 302 | candidats bruts |
| ↳ une seule lettre/nombre `(א)`, `(קלא)` | 1 344 232 | marqueurs de notes et de seifim (קובץ שיטות קמאי, משנה ברורה/שער הציון, ערוך השולחן…), **pas des citations** |
| ↳ plus de 8 mots | 244 516 | prose, hors périmètre |
| ↳ courts (≤ 8 mots) | 3 866 554 | périmètre analysé |
| `<a href="otzaria://inline-link?path=…&index=…">` | 19 296 | un seul livre (שמירת שבת כהלכתה - מפתחות → … - א) |
| `<a class="refLink" data-ref="…">` (Sefaria) | 151 | surtout « Kol Yaakov » (liens « לחץ כאן ») |

`line.heRef` couvre environ 80 % des lignes. Pour les livres Sefaria, le format est régulier : `{titre} {sections séparées par ", "}`, par exemple `בראשית א, ד`, `שבת כא:, ה`, `שולחן ערוך, אורח חיים רנא, ב`, `טור, אורח חיים, קצג, א`. Cela suffit pour construire un index générique *tuple de sections → première ligne*.

## 2. Résultats par type

### A. Citations explicites (livre + lieu dans la parenthèse) : 1,17M résolues

| Famille | Exemple | Reconnues | Résolues | Concordance avec `link`¹ |
|---|---|---:|---:|---:|
| Tanakh verset | `(בראשית א, ד)` `(ישעיה מ, ג-ה)` `(משלי כ"ט:כ"ו)` | 524k | **98,5 %** | 99,4 % |
| Tanakh chapitre | `(בראשית לד)` | 152k | 96,4 % | 99,0 % |
| Bavli daf + amud | `(שבת כא:)` `(ב"ק כב ע"ב)` `(ברכות ח א)` | 284k | **99,7 %** | 97,7 % |
| Bavli daf sans amud | `(שבת דף כא)` | 132k | 99,3 % | 83 % (amud inconnu) |
| Mishna | `(אבות פ"ג מי"ג)` `(כלים פ' י"ז מי"ג)` | 47k | 71–89 % | 99,3 % |
| Midrash Rabba | `(ב"ר פ"ח)` `(במ"ר פכ"א א')` | 23k | 93,5 % | 96,6 % |
| SA siman / seif | `(או"ח סי' רנא ס"ב)` | 20k | 99,6 % / 58 % | 70,6 % / 98,2 % |
| Yerushalmi | `(ירושלמי ברכות פ"א ה"ב)` | 4k | 93 % | 100 % |
| Rambam | `(רמב"ם פי"ב מהל' שבת הי"א)` | 1,7k (+6,8k lookbehind) | 89–97 % | 98,5 % |
| Tosafot / Rashi sur Bavli | `(תוס' שבת כא. ד"ה …)` | 6k | 99,5 % | — ² |
| Tur, MB, Tosefta, Yalkut, Tanchuma, Chinuch | | ~8k | 37–100 % | 93–100 % |
| Zohar volume+daf | `(זח"א דף כה.)` | 3,4k | **87 %** via `alt_toc` (structure Vilna) | — |

¹ Parmi les lignes où Sefaria a déjà un lien vers le même livre : pourcentage dont la cible tombe dans la même section (chapitre ou daf).
² La métrique n'est pas adaptée aux heRef à trois niveaux.

**Précision mesurée par la citation (Tanakh)**, sur un échantillon de 20 000 versets résolus :
- 83 % : les mots juste avant ou après la référence sont dans le verset ;
- 15 % : pas de citation vérifiable à proximité ;
- 1,9 % : la citation correspond à un autre verset. À la relecture, ce sont surtout des **erreurs des textes source** (`ירמיה כט, כג` pour `כג, כט`) ou des plages de versets, pas des erreurs du parser.

**Bavli** (contrôle par la citation, vocabulaire du daf entier) : 54 % de concordance, contre 3,8 % sur un daf tiré au hasard. Le signal est très fort.

### B. Nom du livre juste AVANT la parenthèse (lookbehind) : +333k résolues

Exemples : `בביצה (דף יז)`, `ובחולין (דף קלט)`, `בתוס' מנחות (צ"ה.)`, `בירושלמי שבת (פ"י ה"ה)`, `הרמב"ם (פ"ג מהל' שלוחין ה"ז)`, `בטור יו"ד (סי' צט)`, `בזוה"ק ח"ב (קלה.)`.
- Bavli : 275k cas, 99,4 % résolus. 39 % de concordance par la citation, contre 2 % pour le témoin. Les échantillons relus sont corrects.
- **Noms de perek** (`בפ' מרובה (דף עד)`, `בפרק כל כתבי (דף קיח.)`) : 304 noms extraits de `alt_toc_structure.key='Chapters'`, 8 ambigus. 15,7k cas, 99,5 % résolus, échantillons corrects.
- Tanakh en lookbehind : seulement **55 %** de concordance. Il y a des faux positifs du type `נחלת יהושע (ס' כ"ה)` ou `זוהר ויקרא (ט.)`. Il faudrait des garde-fous ou une validation par la citation.
- SA en lookbehind : `חתם סופר או"ח (סי' ע"ט)` désigne une responsa, pas le SA (voir §3).

### C. Références contextuelles

| Type | Volume | Verdict |
|---|---:|---|
| `(שם …)` dans la **même ligne** que la citation précédente | ~50k Tanakh + ~30k Bavli | ✅ fiable : 80 % confirmés par la citation, autant que l'explicite |
| `(שם …)` qui renvoie à une citation des **lignes précédentes** | ~45k | ❌ 23 % seulement, ne pas lier |
| `(שם N)` en Tanakh : chapitre N ou verset N ? | 26k | ambigu : la citation tranche pour « chapitre N » 2 fois plus souvent ; à résoudre par la citation |
| `(דף X)` dans un commentaire sur un traité, sans autre indice | 21k | ✅ traité de base (`… על <traité>`), échantillons corrects |
| `(דף X)` + « dernier traité cité » dans les lignes précédentes | 34k | ❌ faux positifs (pages du Rif, etc.) |
| `(לעיל/לקמן …)` | ~45k | partiel : dans les commentaires, il faut viser le **texte de base**. `(… הערה 407)` n'est pas résoluble (pas de structure). 21 % résolus dans le livre lui-même |
| `(סי' X)` / `(ס"ק X)` sans livre | ~160k | seulement si le livre a lui-même des simanim, ou par lookbehind |

### D. Autres sources de liens

- `otzaria://inline-link` : **100 %** résolus (`index` en base 1 = `lineIndex + 1` dans le livre de `path`). C'est trivial.
- `refLink` Sefaria (151) : à résoudre à l'import avec `resolveRefs` (refs anglaises), puis retirer la balise.
- Affiner un chapitre en verset : pour `(בראשית לד)`, la citation identifie un verset unique dans **54 %** des cas.

## 3. Ambiguïtés identifiées (à traiter explicitement)

1. `מ"א` / `מ"ב` : מלכים א/ב, מגן אברהם ou משנה ברורה. On tranche par la forme du lieu (`ב, ו` → Tanakh ; `סי' … ס"ק` → MA/MB).
2. `או"ח` / `יו"ד` / `חו"מ` / `אה"ע` seuls : SA, Tur, ou un livre organisé selon le Tur (responsa : `או"ח ח"א סי'` = Igrot Moshe…). C'est pourquoi la concordance SA/siman n'est que de 70 %. `ס"ק` sans nom de commentateur → **ne pas lier**.
3. `(מגילה פ"א)` : perek 1 ou daf 81 ? Sans amud, je choisis perek (→ Mishna).
4. `(ברכות ח א)` : daf 8a ou Mishna 8:1 ? Si le second terme est א/ב → daf.
5. Le Yalkut a deux numérotations de remez (Torah / Nakh).
6. Daf sans amud : on lie au daf (amud א), ou on choisit l'amud par la citation.
7. Normaliser le ktiv (yod/vav) crée des faux positifs (`הובא למעלה` → « מעילה ») : utiliser une liste d'alias explicite.
8. Formes « fusionnées » à gérer : `פי"ג`, `מי"ג`, `הי"א`, `סע"ב`, `הל"ט` (halacha 9, que mon proto lit encore à tort comme 39).

## 4. Volume final estimé (prototype, sans tuning)

- ~1,17M citations explicites + ~0,33M en lookbehind + ~0,1M contextuelles fiables : **≈ 1,6M liens**, sur ~780k lignes et ~4 400 livres sources.
- Précision attendue en ne gardant que les classes ✅ : de l'ordre de **97–99 %**. L'essentiel des erreurs restantes vient des textes source eux-mêmes.

## 5. Recommandations

1. **Lier** : explicites (A), lookbehind Bavli, Rambam, Tur, Tosafot, Yerushalmi et perek-name (B), `שם` dans la même ligne, `(דף X)` dans un commentaire de traité, `otzaria://`.
2. **Valider par la citation avant de lier** : lookbehind Tanakh, `(שם N)`, affinage chapitre → verset, choix de l'amud.
3. **Ne pas lier** : `שם` qui traverse plusieurs lignes, « dernier traité cité » au-delà de la ligne, `ס"ק` sans commentateur, `או"ח ח"X`, notes `הערה N`.
4. Stocker un champ `confidence` (explicite / lookbehind / contexte / validé par citation) pour pouvoir filtrer côté app sans régénérer.
