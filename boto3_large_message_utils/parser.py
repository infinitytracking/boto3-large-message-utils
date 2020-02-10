import json
from json import JSONDecodeError

import boto3

from boto3_large_message_utils.utils.compression import (
    decode_and_decompress_string,
    decompress_string,
)
from boto3_large_message_utils.exceptions import DecompressionError


class LargeMessageParser:
    def __init__(self, session=None):
        if session:
            self.s3 = session.client("s3")
        self.s3 = boto3.client("s3")

    def parse_json(self, json_message):
        if not isinstance(json_message, dict):
            raise ValueError('"message" argument expects type "dict"')
        try:
            if json_message.get("compressedMessage"):
                return decode_and_decompress_string(
                    json_message.get("compressedMessage")
                )
            if json_message.get("bucket"):
                return self._retrieve_message_from_s3(
                    bucket=json_message["bucket"],
                    key=json_message["key"],
                    compressed=json_message["compressed"],
                )
            return json_message
        except (KeyError, JSONDecodeError):
            return json_message
        except DecompressionError:
            raise DecompressionError('"message" could not be decompressed')

    def parse(self, message):
        if not isinstance(message, str):
            raise ValueError('"message" argument expects type "str"')
        try:
            json_message = json.loads(message)
            return self.parse_json(json_message)
        except (KeyError, JSONDecodeError):
            return message
        except DecompressionError:
            raise DecompressionError('"message" could not be decompressed')

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
