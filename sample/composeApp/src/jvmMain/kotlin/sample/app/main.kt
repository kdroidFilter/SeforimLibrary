import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.unit.Density
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application
import androidx.compose.ui.window.rememberWindowState
import sample.app.App
import java.awt.Dimension
import java.util.Locale

fun main() {
    Locale.setDefault(Locale.Builder().setLanguage("he").setRegion("IL").build())
    application {
        Window(
            title = "sample",
            state = rememberWindowState(width = 1280.dp, height = 720.dp),
            onCloseRequest = ::exitApplication,
        ) {
            window.minimumSize = Dimension(350, 600)
            CompositionLocalProvider(LocalDensity provides Density(density = 1f, fontScale = 1.0f)) {
                App()
            }
        }
    }
}
