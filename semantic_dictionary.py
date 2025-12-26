FINANCING_MAP = {
    "cash": "Cash",
    "conventional": "Conventional",
    "fha": "FHA",
    "va": "VA",
    "owner finance": "Owner Financing",
}

def normalize_financing(value: str | None) -> str | None:
    if not value:
        return None
    v = value.strip().lower()
    return FINANCING_MAP.get(v, value)
