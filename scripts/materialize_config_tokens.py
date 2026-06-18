#!/usr/bin/env python3
"""Create missing file-backed token secrets declared by config.yaml."""

from __future__ import annotations

import os
import re
import secrets
import sys
from pathlib import Path


TOP_LEVEL_RE = re.compile(r"^[A-Za-z0-9_-]+:\s*(?:.*)?$")
CHILD_RE = re.compile(r"^  ([A-Za-z0-9._-]+):\s*(?:#.*)?$")
CONTEXT_RE = re.compile(r"^    context:\s*([A-Za-z0-9._-]+|\"[^\"]+\"|'[^']+')\s*(?:#.*)?$")
TOKEN_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def section_lines(lines: list[str], section: str) -> list[str]:
    section_re = re.compile(rf"^{re.escape(section)}:\s*(?:#.*)?$")
    start = next((idx for idx, line in enumerate(lines) if section_re.match(line)), None)
    if start is None:
        return []

    end = len(lines)
    for idx in range(start + 1, len(lines)):
        if TOP_LEVEL_RE.match(lines[idx]):
            end = idx
            break
    return lines[start + 1 : end]


def token_names(lines: list[str], section: str) -> list[str]:
    names: list[str] = []
    has_context: dict[str, bool] = {}
    current: str | None = None

    for line in section_lines(lines, section):
        child = CHILD_RE.match(line)
        if child:
            current = child.group(1)
            if not TOKEN_NAME_RE.match(current):
                raise ValueError(
                    f"{section}.{current} may contain only letters, digits, dot, underscore, and hyphen"
                )
            if current in has_context:
                raise ValueError(f"Duplicate token config entry: {section}.{current}")
            names.append(current)
            has_context[current] = False
            continue

        if current is not None and CONTEXT_RE.match(line):
            has_context[current] = True

    missing_context = [name for name in names if not has_context[name]]
    if missing_context:
        joined = ", ".join(f"{section}.{name}.context" for name in missing_context)
        raise ValueError(f"Missing required config entries: {joined}")

    return names


def create_token_file(path: Path) -> bool:
    if path.exists():
        if not path.is_file():
            raise ValueError(f"Token path exists but is not a regular file: {path}")
        os.chmod(path, 0o600)
        return False

    token = secrets.token_urlsafe(32)
    tmp_path = path.with_name(f".{path.name}.{secrets.token_hex(8)}")
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(token)
            handle.write("\n")
        os.replace(tmp_path, path)
        os.chmod(path, 0o600)
    except Exception:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise
    return True


def materialize(label: str, names: list[str], directory: Path, filter_name: str | None) -> None:
    if filter_name is not None and filter_name not in names:
        raise ValueError(f"{label} token is not declared in config.yaml: {filter_name}")

    selected = [filter_name] if filter_name is not None else names
    directory.mkdir(parents=True, exist_ok=True)
    os.chmod(directory, 0o700)

    for name in selected:
        path = directory / f"{name}.token"
        created = create_token_file(path)
        action = "created" if created else "exists"
        print(f"{label} token file {action}: {path}")


def main() -> int:
    config_path = Path(os.environ.get("CONFIG_FILE", "config.yaml"))
    api_dir = Path(os.environ.get("API_TOKEN_DIR", ".secrets/api"))
    mcp_dir = Path(os.environ.get("MCP_TOKEN_DIR", ".secrets/mcp"))
    token_kind = os.environ.get("TOKEN_KIND")
    token_name = os.environ.get("TOKEN_NAME")

    if token_kind not in {None, "", "api", "mcp"}:
        print("TOKEN_KIND must be one of: api, mcp", file=sys.stderr)
        return 1
    if token_name and not TOKEN_NAME_RE.match(token_name):
        print(
            "TOKEN_NAME may contain only letters, digits, dot, underscore, and hyphen",
            file=sys.stderr,
        )
        return 1

    try:
        lines = config_path.read_text(encoding="utf-8").splitlines()
        api_names = token_names(lines, "api_tokens")
        mcp_names = token_names(lines, "mcp_tokens")

        print(f"Config file: {config_path}")
        if token_kind in {None, "", "api"}:
            materialize("API", api_names, api_dir, token_name if token_kind == "api" else None)
        if token_kind in {None, "", "mcp"}:
            materialize("MCP", mcp_names, mcp_dir, token_name if token_kind == "mcp" else None)
    except FileNotFoundError:
        print(f"Config file not found: {config_path}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
