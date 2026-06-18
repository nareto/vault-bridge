#!/usr/bin/env python3
"""Ensure config.yaml declares a file-backed token and its target context."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path


TOP_LEVEL_RE = re.compile(r"^[A-Za-z0-9_-]+:\s*(?:#.*)?$")
PEER_RE = re.compile(r"^  [A-Za-z0-9._-]+:\s*(?:#.*)?$")


def main() -> int:
    config_path = Path(os.environ["CONFIG_FILE"])
    section = os.environ["TOKEN_CONFIG_SECTION"]
    token_kind = os.environ["TOKEN_CONFIG_KIND"]
    name = os.environ["TOKEN_NAME"]
    context = os.environ["TOKEN_CONTEXT"]

    if not config_path.exists():
        print(f"Config file not found: {config_path}", file=sys.stderr)
        print(
            "Create config.yaml first or pass CONFIG_FILE=/path/to/config.yaml.",
            file=sys.stderr,
        )
        return 1

    text = config_path.read_text()
    lines = text.splitlines(keepends=True)

    def section_bounds(section: str) -> tuple[int, int] | None:
        section_re = re.compile(rf"^{re.escape(section)}:\s*(?:#.*)?$")
        start = next(
            (idx for idx, line in enumerate(lines) if section_re.match(line)),
            None,
        )
        if start is None:
            return None
        end = len(lines)
        for idx in range(start + 1, len(lines)):
            if TOP_LEVEL_RE.match(lines[idx]):
                end = idx
                break
        return start, end

    def ensure_top_section(section: str, before: str | None = None) -> None:
        if section_bounds(section) is not None:
            return
        before_bounds = section_bounds(before) if before else None
        insert_at = before_bounds[0] if before_bounds else len(lines)
        block = [f"{section}:\n"]
        if insert_at > 0 and lines[insert_at - 1].strip():
            block.insert(0, "\n")
        lines[insert_at:insert_at] = block

    def child_bounds(section: str, child: str) -> tuple[int, int] | None:
        bounds = section_bounds(section)
        if bounds is None:
            return None
        start, end = bounds
        child_re = re.compile(rf"^  {re.escape(child)}:\s*(?:#.*)?$")
        for idx in range(start + 1, end):
            if child_re.match(lines[idx]):
                child_end = end
                for peer_idx in range(idx + 1, end):
                    if PEER_RE.match(lines[peer_idx]):
                        child_end = peer_idx
                        break
                return idx, child_end
        return None

    def append_child(section: str, child_lines: list[str]) -> None:
        bounds = section_bounds(section)
        if bounds is None:
            raise RuntimeError(f"missing section after ensure: {section}")
        _, end = bounds
        block = list(child_lines)
        if end > 0 and lines[end - 1].strip():
            block.insert(0, "\n")
        lines[end:end] = block

    ensure_top_section(section, before="mcp_tokens" if section == "api_tokens" else "indexer")

    token_action = "unchanged"
    token = child_bounds(section, name)
    if token is None:
        append_child(section, [f"  {name}:\n", f"    context: {context}\n"])
        token_action = "added"
    else:
        token_start, token_end = token
        context_re = re.compile(r"^    context:\s*")
        context_line = next(
            (
                idx
                for idx in range(token_start + 1, token_end)
                if context_re.match(lines[idx])
            ),
            None,
        )
        replacement = f"    context: {context}\n"
        if context_line is None:
            lines[token_start + 1 : token_start + 1] = [replacement]
            token_action = "updated"
        elif lines[context_line] != replacement:
            lines[context_line] = replacement
            token_action = "updated"

    ensure_top_section("contexts", before="context_assembly")
    context_added = False
    if child_bounds("contexts", context) is None:
        append_child(
            "contexts",
            [
                f"  {context}:\n",
                "    read: []\n",
                "    create: []\n",
                "    edit: []\n",
            ],
        )
        context_added = True

    after_text = "".join(lines)
    if after_text and not after_text.endswith("\n"):
        after_text += "\n"
    if after_text != text:
        config_path.write_text(after_text)

    print(f"Config file: {config_path}")
    print(f"- {section}.{name}.context: {context} ({token_action})")
    if context_added:
        print(f"- contexts.{context}: added as deny-all (empty read/create/edit rules)")
        print(f"  Edit contexts.{context} in config.yaml before granting this client access.")
    else:
        print(f"- contexts.{context}: already exists")
    if after_text != text:
        print(
            f"Config changes are hot-reloaded for {token_kind} mappings and context policies; "
            f"wait for the next reload or send SIGHUP before relying on them."
        )
    else:
        print("Config already matched; rotating this token file takes effect without restart.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
