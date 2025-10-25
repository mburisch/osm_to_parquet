from __future__ import annotations


def join_path(root: str, *args: str) -> str:
    if args:
        root = root.rstrip("/")
    parts = [root] + [p.lstrip("/") for p in args]
    return "/".join(parts)
