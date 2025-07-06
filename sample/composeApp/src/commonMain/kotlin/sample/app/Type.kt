package sample.app

import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Typography
import androidx.compose.runtime.Composable
import org.jetbrains.compose.resources.Font
import seforimlibrary.sample.composeapp.generated.resources.Res
import seforimlibrary.sample.composeapp.generated.resources.noto


@Composable
fun notoFont() = Typography(
    displayLarge = MaterialTheme.typography.displayLarge.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(
        Font(
            Res.font.noto
        )
    )
    ),
    displayMedium = MaterialTheme.typography.displayMedium.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(
        Font(Res.font.noto)
    )
    ),
    displaySmall = MaterialTheme.typography.displaySmall.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(
        Font(
            Res.font.noto
        )
    )
    ),
    headlineLarge = MaterialTheme.typography.headlineLarge.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(
        Font(Res.font.noto)
    )
    ),
    headlineMedium = MaterialTheme.typography.headlineMedium.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(
        Font(Res.font.noto)
    )
    ),
    headlineSmall = MaterialTheme.typography.headlineSmall.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(
        Font(Res.font.noto)
    )
    ),
    titleLarge = MaterialTheme.typography.titleLarge.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(Font(Res.font.noto))),
    titleMedium = MaterialTheme.typography.titleMedium.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(
        Font(
            Res.font.noto
        )
    )
    ),
    titleSmall = MaterialTheme.typography.titleSmall.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(Font(Res.font.noto))),
    bodyLarge = MaterialTheme.typography.bodyLarge.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(Font(Res.font.noto))),
    bodyMedium = MaterialTheme.typography.bodyMedium.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(Font(Res.font.noto))),
    bodySmall = MaterialTheme.typography.bodySmall.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(Font(Res.font.noto))),
    labelLarge = MaterialTheme.typography.labelLarge.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(Font(Res.font.noto))),
    labelMedium = MaterialTheme.typography.labelMedium.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(
        Font(
            Res.font.noto
        )
    )
    ),
    labelSmall = MaterialTheme.typography.labelSmall.copy(fontFamily = androidx.compose.ui.text.font.FontFamily(Font(Res.font.noto))),
)