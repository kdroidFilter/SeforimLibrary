package io.github.kdroidfilter.seforimlibrary.env

/**
 * Returns the value of the given environment variable, or null if unavailable
 * on the current platform or not set.
 */
expect fun getEnvironmentVariable(name: String): String?

