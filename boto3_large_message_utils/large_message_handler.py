import json
from json import JSONDecodeError

import boto3

from boto3_large_message_utils.utils import (
    compress_and_encode_string,
    compress_string,
    decode_and_decompress_string,
    decompress_string,
    get_size_of_string_in_bytes,
    generate_s3_object_key,
    get_message_attributes_size_in_bytes,
    append_message_size_attribute,
)
from boto3_large_message_utils.exceptions import CompressionError, DecompressionError
from boto3_large_message_utils.constants import DEFAULT_MESSAGE_SIZE_THRESHOLD


class LargeMessageHandler:
    def __init__(
        self,
        s3_bucket_for_cache,
        s3_object_prefix=None,
        compress=False,
        message_size_threshold=DEFAULT_MESSAGE_SIZE_THRESHOLD,
        session=None,
    ):
        self.s3_bucket_for_cache = s3_bucket_for_cache
        self.s3_object_prefix = s3_object_prefix
        self.compress = compress
        self.message_size_threshold = message_size_threshold

        if session:
            self.s3 = session.client("s3")
        self.s3 = boto3.client("s3")

    def submit_message(self, message, message_attributes: dict = None):
        if message_attributes:
            return self._handle_message_with_message_attributes(message, message_attributes)
        return self._handle_message(message)

    def _handle_message(self, message: str) -> str:
        if not isinstance(message, str):
            raise ValueError('"message" argument expects type "str"')

        message_size = get_size_of_string_in_bytes(message)

        if message_size < self.message_size_threshold:
            return message

        if self.compress:
            compressed_message = self._get_compressed_message_body(message)
            compressed_message_size = get_size_of_string_in_bytes(compressed_message)

            if compressed_message_size < self.message_size_threshold:
                return compressed_message

        cached_message_body = self._store_message_in_s3(message)
        return cached_message_body

    def _handle_message_with_message_attributes(self, message: str, message_attributes: dict) -> (str, dict):
        if not isinstance(message, str):
            raise ValueError('"message" argument expects type "str"')
        if not isinstance(message_attributes, dict):
            raise ValueError('"message_attributes" argument expects type "dict"')

        message_size = get_size_of_string_in_bytes(message)
        message_attributes_size = get_message_attributes_size_in_bytes(message_attributes, self.message_size_threshold)

        if message_size + message_attributes_size < self.message_size_threshold:
            return message, message_attributes

        updated_message_attributes = append_message_size_attribute(message_attributes, message_size)

        if self.compress:
            compressed_message_body = self._get_compressed_message_body(message)
            compressed_message_size = get_size_of_string_in_bytes(compressed_message_body)

            if compressed_message_size + message_attributes_size < self.message_size_threshold:
                return compressed_message_body, updated_message_attributes

        cached_message_body = self._store_message_in_s3(message)
        return cached_message_body, updated_message_attributes

    def parse_message(self, message):
        if not isinstance(message, str):
            raise ValueError('"message" argument expects type "str"')
        try:
            json_message = json.loads(message)
            if json_message.get("compressedMessage"):
                return decode_and_decompress_string(json_message.get("compressedMessage"))
            if json_message.get("bucket"):
                return self._retrieve_message_from_s3(
                    bucket=json_message["bucket"], key=json_message["key"], compressed=json_message["compressed"]
                )
            return message
        except (KeyError, JSONDecodeError):
            return message
        except DecompressionError:
            raise DecompressionError('"message" could not be decompressed')

    @staticmethod
    def _get_compressed_message_body(message: str) -> str:
        try:
            compressed_message_contents = compress_and_encode_string(message)
            return json.dumps({"compressedMessage": compressed_message_contents})
        except (ValueError, CompressionError):
            raise CompressionError('"message" could not be compressed')

    @staticmethod
    def _get_cached_message_body(bucket: str, key: str, compressed: bool = False) -> str:
        if not isinstance(bucket, str):
            raise ValueError('"bucket" argument expects type "str"')
        if not isinstance(key, str):
            raise ValueError('"key" argument expects type "str"')
        if compressed and not isinstance(compressed, bool):
            raise ValueError('"compressed" argument expects type "bool"')
        return json.dumps({"bucket": bucket, "key": key, "compressed": compressed})

    def _store_message_in_s3(self, message: str) -> str:
        try:
            s3_object_key = generate_s3_object_key(prefix=self.s3_object_prefix)
            cached_message_body = self._get_cached_message_body(
                self.s3_bucket_for_cache, s3_object_key, compressed=self.compress
            )
            if self.compress:
                message = compress_string(message)
            else:
                message = message.encode("utf-8")
            self.s3.put_object(
                Bucket=self.s3_bucket_for_cache, Body=message, Key=s3_object_key
            )

            return cached_message_body
        except CompressionError:
            raise CompressionError('"message" could not be compressed')

    def _retrieve_message_from_s3(self, bucket, key, compressed=False):
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read()
            if compressed:
                return decompress_string(body)

            return body.decode("utf-8")
        except Exception as e:
            print("Error retrieving message from S3")
            raise e
