from textual.theme import Theme

from valkyrie.palette import DARK, PALETTES, Palette


def textual_theme(palette: Palette) -> Theme:
    return Theme(
        name=palette.name,
        primary=palette.accent,
        secondary=palette.border_focus,
        accent=palette.accent,
        foreground=palette.text,
        background=palette.background,
        surface=palette.surface,
        panel=palette.panel,
        success=palette.finished,
        warning=palette.warning,
        error=palette.error,
        dark=palette.dark,
        variables={
            "border": palette.border_focus,
            "border-blurred": palette.border,
            "text-muted": palette.muted,
            "foreground-muted": palette.muted,
            "block-cursor-background": palette.cursor,
            "block-cursor-foreground": palette.text,
            "block-cursor-text-style": "none",
            "block-cursor-blurred-background": palette.cursor_blurred,
            "block-cursor-blurred-foreground": palette.text,
            "block-cursor-blurred-text-style": "none",
            "block-hover-background": palette.hover,
            "scrollbar": palette.border,
            "scrollbar-hover": palette.faint,
            "scrollbar-active": palette.accent,
            "scrollbar-background": palette.background,
            "scrollbar-background-hover": palette.background,
            "scrollbar-background-active": palette.background,
            "scrollbar-corner-color": palette.background,
            "input-selection-background": f"{palette.accent} 30%",
            "screen-selection-foreground": palette.text,
        },
    )


_active = DARK


def palette() -> Palette:
    return _active


def use(name: str) -> None:
    global _active
    _active = PALETTES[name]
