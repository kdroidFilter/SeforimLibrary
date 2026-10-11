package io.github.kdroidfilter.seforimlibrary.reflinks

/**
 * Translates the English references Sefaria embeds in its texts (`<a class="refLink" data-ref="Tosafot on Bava Kamma 22b:2">`)
 * into a Hebrew title and heRef sections, from the titles of the Sefaria export schemas.
 */
class SefariaRefParser(books: Collection<Book>) {

    /**
     * One book of the export: every English title variant, its Hebrew title, and the English → Hebrew names of the
     * nodes of a complex text ("Yoreh De'ah" → "יורה דעה").
     */
    data class Book(val enTitles: List<String>, val heTitle: String, val nodes: Map<String, String> = emptyMap())

    data class Ref(val heTitle: String, val sections: List<Section>)

    private class Entry(val heTitle: String, val nodes: Map<String, String>)

    private val byTitle: Map<String, Entry> = books.flatMap { b ->
        val entry = Entry(b.heTitle, b.nodes.mapKeys { normalize(it.key) })
        b.enTitles.map { normalize(it) to entry }
    }.toMap()
    private val longestTitle = byTitle.keys.maxOfOrNull { it.length } ?: 0
    private val address = Regex("^(\\d+)([ab])?$")

    fun parse(dataRef: String): Ref? {
        val ref = normalize(dataRef)
        // Longest English title that the reference starts with, followed by a separator
        val (book, rest) = (minOf(longestTitle, ref.length) downTo 1).firstNotNullOfOrNull { n ->
            byTitle[ref.substring(0, n)]
                ?.takeIf { n == ref.length || ref[n] == ' ' || ref[n] == ',' }
                ?.let { it to ref.substring(n) }
        } ?: return null

        var tail = rest.trimStart(',', ' ')
        val sections = mutableListOf<Section>()
        // Named nodes of a complex text: ", Yoreh De'ah 276:67:1"
        while (true) {
            val node = book.nodes.keys.filter { tail.startsWith(it) }.maxByOrNull { it.length } ?: break
            sections += Section.Name(book.nodes.getValue(node))
            tail = tail.substring(node.length).trimStart(',', ' ')
        }
        if (tail.isNotEmpty()) {
            // A range ("1:2-5") links to its start
            for (token in tail.substringBefore('-').split(':')) {
                val m = address.find(token.trim()) ?: return null
                val n = m.groupValues[1].toInt()
                sections += when (m.groupValues[2]) {
                    "a" -> Section.Daf(n, Section.Side.A)
                    "b" -> Section.Daf(n, Section.Side.B)
                    else -> Section.Num(n)
                }
            }
        }
        return Ref(book.heTitle, sections)
    }

    private fun normalize(s: String): String = s.replace('_', ' ').trim().lowercase()
}
