import uuid


def generate_s3_object_key(prefix: str = None) -> str:
    if prefix and not isinstance(prefix, str):
        raise ValueError('"string_to_decode_and_decompress" argument expects type "str"')
    key = str(uuid.uuid4())
    if prefix and len(prefix) > 0:
        key = prefix.strip("/") + "/" + key
    return key
