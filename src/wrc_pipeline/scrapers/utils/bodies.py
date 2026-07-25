BODIES: dict[int, str] = {
    1: "Equality Tribunal",
    2: "Employment Appeals Tribunal",
    3: "Labour Court",
    15376: "Workplace Relations Commission",
}


def body_slug(name: str) -> str:
    return name.lower().replace(" ", "_")
