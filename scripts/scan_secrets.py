#!/usr/bin/env python3
"""开源仓库敏感信息扫描器（只报路径/行号/类别，不回显原值）。

用法：
    python3 scripts/scan_secrets.py                     # 扫描当前目录（跳过 .git/.venv/assets）
    python3 scripts/scan_secrets.py packages docs       # 只扫指定路径
    python3 scripts/scan_secrets.py --tracked-only      # 只扫 git 跟踪的文件
    python3 scripts/scan_secrets.py --blocklist .scan-blocklist   # 额外精确黑名单（每行一个敏感值，gitignore）

命中返回码 1；无命中 0。CI 用 --tracked-only + 通用规则；本地可加 --blocklist 精确扫描真实群号/QQ。
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

SKIP_DIRS = {".git", ".venv", "venv", "__pycache__", "node_modules", "dist", "build", "assets", ".pytest_cache", ".ruff_cache"}
SKIP_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico", ".ttf", ".ttc", ".otf", ".woff", ".woff2", ".zip", ".adx", ".so", ".pyc", ".pdf", ".db", ".sqlite"}

PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("private-path", re.compile(r"/(?:home|Users)/[A-Za-z0-9._-]+/")),
    ("private-domain", re.compile(r"\.internal\b|ts\.net\b", re.IGNORECASE)),
    ("absolute-app-path", re.compile(r"[\"']/app/")),
    ("token", re.compile(r"\b(?:sk-[A-Za-z0-9_-]{16,}|ghp_[A-Za-z0-9]{20,}|xox[baprs]-[A-Za-z0-9-]{10,})")),
    ("private-key", re.compile(r"BEGIN (?:RSA |OPENSSH |EC |PGP )?PRIVATE KEY")),
    ("secret-assignment", re.compile(r"(?i)(token|secret|passwd|password|api[_-]?key)\s*[:=]\s*[\"'][^\"'<>{}]{6,}[\"']")),
    ("private-ip", re.compile(r"\b(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}|100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.\d{1,3}\.\d{1,3})\b")),
    ("qq-or-group-id", re.compile(r"(?i)(?:qq|uin|superuser|群号|群)\D{0,16}(\d{6,12})")),
]

ALLOW_SUBSTRINGS = (
    "example.invalid",
    "example.com",
    "127.0.0.1",
    "0.0.0.0",
    "REPLACE_ME",
    "<群号>",
    "10000",
    "100000",
)


def iter_files(paths: list[str], tracked_only: bool) -> list[Path]:
    self_path = Path(__file__).resolve()
    if tracked_only:
        out = subprocess.run(["git", "ls-files", "-z"], capture_output=True, check=True).stdout
        files = [Path(p) for p in out.decode().split("\0") if p]
        roots = [Path(p) for p in paths]
        if roots:
            files = [f for f in files if any(f == r or r in f.parents for r in roots)]
        return [f for f in files if f.resolve() != self_path]

    files: list[Path] = []
    for raw in paths:
        root = Path(raw)
        if root.is_file():
            files.append(root)
            continue
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            if any(part in SKIP_DIRS for part in p.parts):
                continue
            if p.suffix.lower() in SKIP_SUFFIXES:
                continue
            if p.resolve() == self_path:
                continue
            files.append(p)
    return files


def load_blocklist(path: Path) -> list[str]:
    if not path.exists():
        return []
    values = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            values.append(line)
    return values


def main() -> int:
    ap = argparse.ArgumentParser(description="scan repo for private data / credentials")
    ap.add_argument("paths", nargs="*", default=["."])
    ap.add_argument("--tracked-only", action="store_true", help="only scan git tracked files")
    ap.add_argument("--blocklist", default=".scan-blocklist", help="optional exact-value blocklist (gitignored)")
    ap.add_argument("--max-hits", type=int, default=200)
    args = ap.parse_args()

    blocklist = load_blocklist(Path(args.blocklist))
    hits = 0
    scanned = 0
    for f in iter_files(args.paths, args.tracked_only):
        if not f.exists():
            continue
        try:
            text = f.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        scanned += 1
        for lineno, line in enumerate(text.splitlines(), 1):
            if any(a in line for a in ALLOW_SUBSTRINGS) and "private-ip" not in "".join(c for c, _ in []):
                # allow explicit placeholder lines; still let token patterns catch real keys
                pass
            for category, pattern in PATTERNS:
                m = pattern.search(line)
                if not m:
                    continue
                if category == "private-ip" and any(a in line for a in ("127.0.0.1", "example.invalid")):
                    continue
                if category == "private-domain" and "<" in line:
                    continue
                print(f"{f}:{lineno}:{category}")
                hits += 1
                if hits >= args.max_hits:
                    print(f"... hit limit ({args.max_hits}) reached", file=sys.stderr)
                    return 1
            for value in blocklist:
                if value and value in line:
                    print(f"{f}:{lineno}:blocklist")
                    hits += 1
                    if hits >= args.max_hits:
                        return 1
    print(f"scanned {scanned} files, {hits} hit(s)", file=sys.stderr)
    return 1 if hits else 0


if __name__ == "__main__":
    sys.exit(main())
