SUPPORTED_COUNTRY_CODES = [
    {"code": "+91", "label": "India", "max_length": 10},
    {"code": "+1", "label": "United States / Canada", "max_length": 10},
    {"code": "+44", "label": "United Kingdom", "max_length": 10},
    {"code": "+61", "label": "Australia", "max_length": 9},
    {"code": "+86", "label": "China", "max_length": 11},
]


def get_supported_country_codes():
    return SUPPORTED_COUNTRY_CODES


def _get_country_config(country_code):
    return next((item for item in SUPPORTED_COUNTRY_CODES if item["code"] == country_code), None)


def normalize_phone_number(phone, default_country_code="+91"):
    """
    Normalize phones into canonical storage format: +<countrycode><number>.
    Examples:
    +91 9876543210 -> +919876543210
    9876543210 -> +919876543210
    +1-415-555-2671 -> +14155552671
    """
    if not phone:
        return phone

    raw = str(phone).strip()
    digits = "".join(ch for ch in raw if ch.isdigit())

    if not digits:
        return raw

    if raw.startswith("+"):
        return f"+{digits}"

    default_config = _get_country_config(default_country_code)
    if default_config and len(digits) == default_config["max_length"]:
        return f"{default_country_code}{digits}"

    for config in SUPPORTED_COUNTRY_CODES:
        country_digits = config["code"][1:]
        local_length = config["max_length"]
        if digits.startswith(country_digits) and len(digits) == len(country_digits) + local_length:
            return f"+{digits}"

    if len(digits) >= 10:
        return f"{default_country_code}{digits[-10:]}"

    return raw


def split_phone_number(phone, default_country_code="+91"):
    """
    Convert canonical/raw storage into UI-friendly parts.
    Returns {"country_code": "+91", "local_number": "9876543210"}.
    """
    normalized = normalize_phone_number(phone, default_country_code=default_country_code)
    if not normalized:
        return {"country_code": default_country_code, "local_number": ""}

    for config in sorted(SUPPORTED_COUNTRY_CODES, key=lambda item: len(item["code"]), reverse=True):
        if normalized.startswith(config["code"]):
            return {
                "country_code": config["code"],
                "local_number": normalized[len(config["code"]):]
            }

    digits = "".join(ch for ch in normalized if ch.isdigit())
    return {
        "country_code": default_country_code,
        "local_number": digits[-10:] if len(digits) >= 10 else digits
    }

