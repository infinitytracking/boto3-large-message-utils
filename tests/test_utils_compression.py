from unittest import TestCase
from unittest.mock import patch

from boto3_large_message_utils.utils.compression import (
    compress_and_encode_string,
    compress_string,
    decode_and_decompress_string,
    decompress_string,
)


class TestCompressString(TestCase):
    @patch("boto3_large_message_utils.utils.compression.gzip.compress")
    def test_gzip_compress_is_called_correctly(self, mock_gzip_compress):
        compress_string("this is a test message")

        mock_gzip_compress.assert_called_once_with(
            "this is a test message".encode("utf-8")
        )

    def test_value_error_is_raised(self):
        with self.assertRaises(ValueError):
            compress_string({"msg": "this method only supports strings"})


class TestCompressAndEncodeString(TestCase):
    @patch(
        "boto3_large_message_utils.utils.compression.gzip.compress",
        return_value=b"mocked gzip bytes",
    )
    def test_gzip_compress_is_called_correctly(self, mock_gzip):
        expected = "bW9ja2VkIGd6aXAgYnl0ZXM="
        actual = compress_and_encode_string("this is a test message")

        mock_gzip.assert_called_once_with(b"this is a test message")
        self.assertEqual(expected, actual)

    def test_value_error_is_raised(self):
        with self.assertRaises(ValueError):
            compress_string({"msg": "this method only supports strings"})


class TestDecompressString(TestCase):
    def test_decompress_string(self):
        test_compressed_string = b"\x1f\x8b\x08\x00T\x11@^\x02\xff+\xc9\xc8,V\x00\xa2D\x85\x92\xd4\xe2\x12\x85\xdc\xd4\xe2\xe2\xc4\xf4T\x00TT\xde\xa2\x16\x00\x00\x00"  # noqa: E501
        expected = "this is a test message"
        actual = decompress_string(test_compressed_string)

        self.assertEqual(expected, actual)

    def test_value_error_is_raised(self):
        with self.assertRaises(ValueError):
            decompress_string({"msg": "this method only supports bytes"})


class TestDecodeAndDecompressString(TestCase):
    def test_decode_and_decompress_string(self):
        test_compressed_and_encoded_string = (
            "H4sIAK4TQF4C/yvJyCxWAKJEhZLU4hKF4pKizLx0ALXWhvwVAAAA"
        )
        expected = "this is a test string"
        actual = decode_and_decompress_string(test_compressed_and_encoded_string)

        self.assertEqual(expected, actual)

    def test_value_error_is_raised(self):
        with self.assertRaises(ValueError):
            compress_string({"msg": "this method only supports strings"})
