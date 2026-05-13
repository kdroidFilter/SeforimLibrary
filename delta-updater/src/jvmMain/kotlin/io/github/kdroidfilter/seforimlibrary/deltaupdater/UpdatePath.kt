package io.github.kdroidfilter.seforimlibrary.deltaupdater

/**
 * Decision returned by [chooseUpdatePath]: either we're up to date, we can
 * apply a chain of deltas, or we need to re-download the full bundle.
 *
 * See `DELTA_UPDATE_PLAN.md` §5.4.
 */
sealed interface UpdatePath {
    data object UpToDate : UpdatePath
    data class Chain(val deltas: List<DeltaEntry>) : UpdatePath
    data class FullBundle(val info: FullBundleEntry) : UpdatePath
}

/**
 * Picks the cheapest path from [localVersion] to [meta.latestVersion]:
 *
 *  - up to date → [UpdatePath.UpToDate]
 *  - local outside retention window → forced [UpdatePath.FullBundle]
 *  - chain total > [fallbackRatio] × full bundle size → [UpdatePath.FullBundle]
 *  - otherwise → [UpdatePath.Chain]
 */
fun chooseUpdatePath(
    localVersion: Int,
    meta: ReleaseMeta,
    fallbackRatio: Double = 0.7,
): UpdatePath {
    if (localVersion >= meta.latestVersion) return UpdatePath.UpToDate

    val oldestSupported = meta.deltas.minOfOrNull { it.fromVersion } ?: return UpdatePath.FullBundle(meta.fullBundle)
    if (localVersion < oldestSupported) return UpdatePath.FullBundle(meta.fullBundle)

    val chain = (localVersion until meta.latestVersion).map { v ->
        meta.deltas.firstOrNull { it.fromVersion == v && it.toVersion == v + 1 }
            ?: return UpdatePath.FullBundle(meta.fullBundle)
    }
    val chainBytes = chain.sumOf { it.totalSize }
    if (chainBytes > meta.fullBundle.size * fallbackRatio) {
        return UpdatePath.FullBundle(meta.fullBundle)
    }
    return UpdatePath.Chain(chain)
}
