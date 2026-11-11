import re
import subprocess
import sys

SKIP = ("docs/reference/", "/vendor/", ".github/", "CHANGELOG.md", "packages/valkyrie-sdk/")
MAX_SENTENCES = 2
MAX_CHARACTERS = 300
ITEM = re.compile(r"^\s*([-*+]|\d+[.)])\s+")
NOT_PROSE = re.compile(r"^(\||#|</?[A-Z])")
NOT_PROSE_START = re.compile(r"^(<|!\[|\{|\[\^)")
QUOTE = re.compile(r"^(\s*>)+ ?")
SENTENCE_END = re.compile(r"[.!?][*_]*(\s|$)")
ABBREVIATIONS = re.compile(r"\b(e\.g|i\.e|vs|etc)\.")
INLINE_CODE = re.compile(r"`[^`]*`")
LINK_TARGET = re.compile(r"\]\([^)]*\)")


def blocks(source: str) -> list[tuple[int, str]]:
    found: list[tuple[int, str]] = []
    start, text = 0, ""
    fence = front_matter = False
    for number, line in enumerate(source.split("\n"), 1):
        if not fence:
            line = QUOTE.sub("", line)
        stripped = line.strip()
        if number == 1 and stripped == "---":
            front_matter = True
            continue
        if front_matter:
            front_matter = stripped != "---"
            continue
        if stripped.startswith(("```", "~~~")):
            fence = not fence
        if (
            fence
            or stripped.startswith(("```", "~~~"))
            or not stripped
            or NOT_PROSE.match(stripped)
            or not text
            and NOT_PROSE_START.match(stripped)
        ):
            text and found.append((start, text))
            text = ""
        elif ITEM.match(line):
            text and found.append((start, text))
            start, text = number, ITEM.sub("", line)
        elif text and line.startswith((" ", "\t")) or text and not ITEM.match(line):
            text += " " + stripped
        elif line.startswith(("    ", "\t")):
            continue
        else:
            start, text = number, stripped
    text and found.append((start, text))
    return found


def violations(path: str, source: str) -> list[str]:
    found = [f"{path}:{number}: em dash" for number, line in enumerate(source.split("\n"), 1) if "\u2014" in line]
    for line, text in blocks(source):
        plain = ABBREVIATIONS.sub("", LINK_TARGET.sub("]", INLINE_CODE.sub("code", text)))
        sentences = len(SENTENCE_END.findall(plain))
        if sentences > MAX_SENTENCES or len(plain) > MAX_CHARACTERS:
            found.append(f"{path}:{line}: {sentences} sentences, {len(plain)} characters")
    return found


def main() -> int:
    files = subprocess.check_output(["git", "ls-files", "*.md", "*.mdx"], text=True).split()
    found: list[str] = []
    for path in files:
        if not any(part in path for part in SKIP):
            with open(path, encoding="utf-8") as file:
                found.extend(violations(path, file.read()))
    for line in found:
        print(line)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
