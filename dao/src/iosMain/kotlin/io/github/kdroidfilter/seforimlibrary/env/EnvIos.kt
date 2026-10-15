package io.github.kdroidfilter.seforimlibrary.env

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString
import platform.posix.getenv

@OptIn(ExperimentalForeignApi::class)
actual fun getEnvironmentVariable(name: String): String? = getenv(name)?.toKString()
