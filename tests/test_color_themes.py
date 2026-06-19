from pathlib import Path

THEME_IDS = ["red", "blue", "green", "gold", "purple"]


def test_layout_exposes_theme_picker_and_persistence():
    source = Path("ui/assets/layout.js").read_text()

    assert "swordfinder:theme:v1" in source
    assert "export const THEMES" in source
    assert "export function applyTheme" in source
    assert "export function getStoredTheme" in source
    # Theme is applied at module load so the palette is set before paint.
    assert "applyTheme(getStoredTheme())" in source
    assert "document.documentElement.setAttribute('data-theme'" in source
    assert "window.localStorage.getItem(THEME_STORAGE_KEY)" in source
    assert "window.localStorage.setItem(THEME_STORAGE_KEY" in source
    # The nav renders a picker wired to applyTheme.
    assert 'id="theme-select"' in source
    assert 'aria-label="Color theme"' in source
    assert "addEventListener('change'" in source
    assert "applyTheme(event.target.value)" in source
    for theme_id in THEME_IDS:
        assert f"id: '{theme_id}'" in source


def test_styles_define_each_theme_palette():
    css = Path("ui/assets/styles.css").read_text()

    for theme_id in THEME_IDS:
        assert f"[data-theme='{theme_id}']" in css
    # Every theme must redefine the core palette variables.
    assert css.count("--accent:") >= len(THEME_IDS)
    assert css.count("--bg:") >= len(THEME_IDS)


def test_accent_colors_derive_from_variables_so_themes_propagate():
    css = Path("ui/assets/styles.css").read_text()

    # The hardcoded accent reds were converted to color-mix on the variables
    # so switching themes retints gradients, glows, borders and focus rings.
    assert "color-mix(in srgb, var(--accent)" in css
    assert "color-mix(in srgb, var(--accent-soft)" in css
    assert "rgba(223, 29, 47" not in css
    assert "rgba(255, 107, 107" not in css


def test_theme_select_is_styled():
    css = Path("ui/assets/styles.css").read_text()

    assert ".theme-select" in css
    assert ".theme-picker" in css
    assert ".sr-only" in css
