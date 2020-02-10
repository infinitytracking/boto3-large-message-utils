import base64
import binascii
import gzip
import uuid
from boto3_large_message_utils.exceptions import CompressionError, DecompressionError


def compress_string(string_to_compress: str) -> bytes:
    if not isinstance(string_to_compress, str):
        raise ValueError('"string_to_compress" argument expects type "str"')
    try:
        return gzip.compress(string_to_compress.encode("utf-8"))
    except OSError:
        raise CompressionError("'string_to_compress' could not be successfully compressed")


def decompress_string(string_to_decompress: bytes) -> str:
    if not isinstance(string_to_decompress, bytes):
        raise ValueError('"string_to_decompress" argument expects type "bytes"')
    try:
        return gzip.decompress(string_to_decompress).decode("utf-8")
    except OSError:
        raise DecompressionError("'string_to_decompress' could not be successfully decompressed")


def compress_and_encode_string(string_to_compress_and_encode: str) -> str:
    if not isinstance(string_to_compress_and_encode, str):
        raise ValueError('"string_to_compress_and_encode" argument expects type "str"')
    try:
        return base64.b64encode(gzip.compress(string_to_compress_and_encode.encode("utf-8"))).decode("utf-8")
    except (OSError, TypeError, binascii.Error):
        raise CompressionError("'string_to_compress_and_encode' could not be successfully compressed and encoded")


def decode_and_decompress_string(string_to_decode_and_decompress: str) -> str:
    if not isinstance(string_to_decode_and_decompress, str):
        raise ValueError('"string_to_decode_and_decompress" argument expects type "str"')
    try:
        return gzip.decompress(base64.b64decode(string_to_decode_and_decompress.encode("utf-8"))).decode("utf-8")
    except (OSError, TypeError, binascii.Error):
        raise DecompressionError("'string_to_decode_and_decompress' could not be successfully decoded and decompressed")


def get_size_of_string_in_bytes(string: str) -> int:
    if isinstance(string, str):
        return len(string.encode("utf-8"))
    raise ValueError('"string" argument expects type "str"')


def generate_s3_object_key(prefix: str = None) -> str:
    if prefix and not isinstance(prefix, str):
        raise ValueError('"string_to_decode_and_decompress" argument expects type "str"')
    key = str(uuid.uuid4())
    if prefix and len(prefix) > 0:
        key = prefix.strip("/") + "/" + key
    return key


def is_base64(possibly_base64):
    try:
        if not isinstance(possibly_base64, str):
            raise ValueError("Argument must be string or bytes")

        possibly_base64 = bytes(possibly_base64, "ascii")
        return base64.b64encode(base64.b64decode(possibly_base64)) == possibly_base64
    except Exception:
        return False
