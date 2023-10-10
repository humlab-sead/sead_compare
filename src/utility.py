from typing import Any


def dget(data: dict, *path: str | list[str], default: Any = None) -> Any:
    if path is None or not data:
        return default

    ps: list[str] = path if isinstance(path, (list, tuple)) else [path]

    d = None

    for p in ps:
        d = dotget(data, p)

        if d is not None:
            return d

    return d or default


def dotexists(data: dict, *paths: list[str]) -> bool:
    for path in paths:
        if dotget(data, path, default="@@") != "@@":
            return True
    return False


def dotexpand(path: str) -> list[str]:
    """Expands paths with ',' and ':'."""
    paths = []
    for p in path.replace(' ', '').split(','):
        if not p:
            continue
        if ':' in p:
            paths.extend([p.replace(":", "."), p.replace(":", "_")])
        else:
            paths.append(p)
    return paths


def dotget(data: dict, path: str, default: Any = None) -> Any:
    """Gets element from dict. Path can be x.y.y or x_y_y or x:y:y.
    if path is x:y:y then element is searched using both x.y.y or x_y_y."""

    for key in dotexpand(path):
        d: dict = data
        for attr in key.split('.'):
            d: dict = d.get(attr) if isinstance(d, dict) else None
            if d is None:
                break
        if d is not None:
            return d
    return default


def dotset(data: dict, path: str, value: Any) -> dict:
    """Sets element in dict. Path can be x.y.y or x_y_y."""

    d: dict = data
    attrs: list[str] = path.replace("_", ".").split('.')
    for attr in attrs[:-1]:
        d: dict = d.setdefault(attr, {})
    d[attrs[-1]] = value

    return data
