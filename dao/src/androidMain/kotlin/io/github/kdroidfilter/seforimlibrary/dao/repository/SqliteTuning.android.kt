package io.github.kdroidfilter.seforimlibrary.dao.repository

// Android counts an app's mapped pages in the memory it kills apps by (small devices first): a small cache, no map
internal actual const val SQLITE_CACHE_KIB: Long = 16000
internal actual const val SQLITE_MMAP_BYTES: Long = 0
