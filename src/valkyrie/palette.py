from dataclasses import dataclass

BRAND = "#284D32"


@dataclass(frozen=True)
class Palette:
    name: str
    dark: bool
    background: str
    surface: str
    panel: str
    text: str
    muted: str
    faint: str
    accent: str
    border: str
    border_focus: str
    cursor: str
    cursor_blurred: str
    hover: str
    track: str
    finished: str
    running: str
    warning: str
    error: str
    stopped: str


DARK = Palette(
    name="valkyrie",
    dark=True,
    background="#111412",
    surface="#161A17",
    panel="#1D231F",
    text="#E6E3DF",
    muted="#9AA7A0",
    faint="#858079",
    accent="#94C7A2",
    border="#2B322D",
    border_focus="#3F7650",
    cursor="#1C2B21",
    cursor_blurred="#171D19",
    hover="#151A16",
    track="#2B322D",
    finished="#6DB283",
    running="#8EA9D6",
    warning="#E0AE5A",
    error="#E08B87",
    stopped="#8F9692",
)

LIGHT = Palette(
    name="valkyrie-light",
    dark=False,
    background="#F3F2F2",
    surface="#EAE8E6",
    panel="#E2E0DD",
    text="#262624",
    muted="#555B57",
    faint="#6E6A66",
    accent=BRAND,
    border="#CFCBC7",
    border_focus="#386B46",
    cursor="#DCE8DF",
    cursor_blurred="#E6EBE7",
    hover="#ECEEEC",
    track="#D6D2CE",
    finished="#2E7346",
    running="#33599B",
    warning="#8A5A00",
    error="#A63A35",
    stopped="#646A66",
)

PALETTES = {palette.name: palette for palette in (DARK, LIGHT)}

ROLES = ("finished", "running", "warning", "error", "stopped", "muted", "faint", "accent")

STATUSES = {
    "PENDING": ("◌", "faint"),
    "BUILDING": ("◔", "running"),
    "IN_PROGRESS": ("●", "running"),
    "EVALUATING": ("◑", "warning"),
    "STOPPING": ("◍", "warning"),
    "STOPPED": ("■", "stopped"),
    "FINISHED": ("✓", "finished"),
    "ERROR": ("✗", "error"),
}


def status_label(value: str) -> str:
    return value.replace("_", " ").lower()
