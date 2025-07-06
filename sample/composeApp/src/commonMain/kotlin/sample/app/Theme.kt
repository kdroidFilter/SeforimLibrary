package sample.app

import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.graphics.Color

private val MonochromeColorScheme = lightColorScheme(
    primary = Color(0xFF333333),
    onPrimary = Color.White,
    primaryContainer = Color(0xFF555555),
    onPrimaryContainer = Color.White,
    secondary = Color(0xFF666666),
    onSecondary = Color.White,
    secondaryContainer = Color(0xFF888888),
    onSecondaryContainer = Color.White,
    tertiary = Color(0xFF666666),
    onTertiary = Color.White,
    tertiaryContainer = Color(0xFF888888),
    onTertiaryContainer = Color.White,
    error = Color(0xFF555555),
    onError = Color.White,
    errorContainer = Color(0xFF555555),
    onErrorContainer = Color.White,
    background = Color.White,
    onBackground = Color(0xFF333333),
    surface = Color(0xFFF5F5F5),
    onSurface = Color(0xFF333333),
    surfaceVariant = Color(0xFFEEEEEE),
    onSurfaceVariant = Color(0xFF333333),
    outline = Color(0xFF888888)
)


@Composable
fun AppTheme(content: @Composable () -> Unit) {
    MaterialTheme(
        colorScheme = MonochromeColorScheme,
        typography = notoFont(),
        content = content
    )
}
