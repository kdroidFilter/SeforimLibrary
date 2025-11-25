package io.github.kdroidfilter.seforimlibrary.env

actual fun getEnvironmentVariable(name: String): String? = System.getenv(name)

