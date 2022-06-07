import re

VALID_FIELD_NAME_PATTERN = re.compile("^[A-Za-z0-9_.\\-\\s]*$")


def validate_field_name_or_throw(field: str) -> str:
    if not field_is_valid(field):
        raise ValueError(f"Field `{field}` contains illegal characters")
    return field


def field_is_valid(field: str) -> bool:
    if not field:
        return False
    match = VALID_FIELD_NAME_PATTERN.match(field)
    return bool(match)


def validate_regexp_or_throw(pattern: str) -> str:
    if not regexp_is_valid(pattern):
        raise ValueError(f"Regexp {pattern} is not valid")
    return pattern


def regexp_is_valid(pattern: str) -> bool:
    try:
        re.compile(pattern)
        is_valid = True
    except re.error:
        is_valid = False
    return is_valid
