import hashlib


def hash_udf_str(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def hash_with_hashlib(value) -> str:
    if type(value) == str:
        return hash_udf_str(value)
    elif type(value) == int:
        return hash_udf_str(str(value))
    else:
        raise Exception(f'Not supported type {type(value)}')
