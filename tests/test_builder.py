from unittest import TestCase
from unittest.mock import patch, Mock

from boto3_large_message_utils.exceptions import CompressionError
from boto3_large_message_utils.builder import LargeMessageBuilder


class TestGetCompressedMessageBody(TestCase):
    def setUp(self):
        self.base = LargeMessageBuilder(s3_bucket_for_cache="test-s3-bucket")

    @patch(
        "boto3_large_message_utils.builder.compress_and_encode_string",
        return_value="<Compressed and Encoded>",
    )
    def test_compressed_message_body_object_is_returned(self, mock_compress_and_encode):
        expected = '{"compressedMessage": "<Compressed and Encoded>"}'
        actual = self.base._get_compressed_message_body("this is a test message")

        mock_compress_and_encode.assert_called_once_with("this is a test message")
        self.assertEqual(expected, actual)

    def test_compression_error_is_raised(self):
        with self.assertRaises(CompressionError):
            self.base._get_compressed_message_body(
                {"msg": "this method only supports strings"}
            )


class TestGetCachedMessageBody(TestCase):
    def setUp(self):
        self.base = LargeMessageBuilder(s3_bucket_for_cache="test-s3-bucket")

    def test_cached_message_body_object_is_returned(self):
        expected = '{"bucket": "test-s3-bucket", "key": "test-object-key", "compressed": false}'
        actual = self.base._get_cached_message_body("test-s3-bucket", "test-object-key")

        self.assertEqual(expected, actual)

    def test_value_error_is_raised_for_bucket_when_not_string(self):
        with self.assertRaises(ValueError) as context:
            self.base._get_cached_message_body(bucket=1, key="string")
        self.assertTrue("bucket" in str(context.exception))

    def test_value_error_is_raised_for_key_when_not_string(self):
        with self.assertRaises(ValueError) as context:
            self.base._get_cached_message_body(bucket="string", key=1)
        self.assertTrue("key" in str(context.exception))


class TestHandleMessage(TestCase):
    def setUp(self):
        self.base = LargeMessageBuilder(s3_bucket_for_cache="test-s3-bucket")

    def test_original_message_is_returned_when_it_is_small_enough(self):
        expected = '{"hello": "world"}'
        actual = self.base._handle_message('{"hello": "world"}')

        self.assertEqual(expected, actual)

    @patch(
        "boto3_large_message_utils.builder.LargeMessageBuilder._get_compressed_message_body"
    )
    def test_compressed_message_is_returned_when_compress_is_true(
        self, mock_get_compressed_message_body
    ):
        mock_get_compressed_message_body.return_value = "<Compressed Message Body>"

        self.base.message_size_threshold = 40
        self.base.compress = True

        test_message = "This is a really long string. 56 characters to be exact."

        expected = "<Compressed Message Body>"
        actual = self.base._handle_message(test_message)

        self.assertEqual(expected, actual)

    @patch("boto3_large_message_utils.builder.LargeMessageBuilder._store_message_in_s3")
    def test_cached_message_is_returned_when_compress_is_false(self, mock_store_in_s3):
        mock_store_in_s3.return_value = '{"bucket": "test-s3-bucket", "key": "test-object-key", "compressed": false}'

        self.base.message_size_threshold = 40
        self.base.compress = False

        test_message = "This is a really long string. 56 characters to be exact."

        expected = '{"bucket": "test-s3-bucket", "key": "test-object-key", "compressed": false}'
        actual = self.base._handle_message(test_message)

        self.assertEqual(expected, actual)

    @patch("boto3_large_message_utils.builder.LargeMessageBuilder._store_message_in_s3")
    @patch(
        "boto3_large_message_utils.builder.LargeMessageBuilder._get_compressed_message_body"
    )
    def test_cached_message_is_returned_when_compress_is_true(
        self, mock_get_compressed_message_body, mock_store_message_in_s3
    ):
        mock_get_compressed_message_body.return_value = "<Compressed Message Body>"
        mock_store_message_in_s3.return_value = (
            '{"bucket": "test-s3-bucket", "key": "test-object-key", "compressed": true}'
        )

        self.base.message_size_threshold = 20
        self.base.compress = True

        test_message = "This is a really long string. 56 characters to be exact."

        expected = (
            '{"bucket": "test-s3-bucket", "key": "test-object-key", "compressed": true}'
        )
        actual = self.base._handle_message(test_message)

        self.assertEqual(expected, actual)

    # TODO: Add some exception handling tests


class TestHandleMessageWithMessageAttributes(TestCase):
    def setUp(self):
        self.base = LargeMessageBuilder(s3_bucket_for_cache="test-s3-bucket")

    def test_original_message_is_returned_when_it_is_small_enough(self):
        test_message = '{"hello": "world"}'
        test_message_attributes = {
            "string": {
                "StringValue": "string",
                "BinaryValue": b"bytes",
                "DataType": "string",
            }
        }

        expected = (
            '{"hello": "world"}',
            {
                "string": {
                    "StringValue": "string",
                    "BinaryValue": b"bytes",
                    "DataType": "string",
                }
            },
        )
        actual = self.base._handle_message_with_message_attributes(
            test_message, test_message_attributes
        )

        self.assertEqual(expected, actual)

    @patch(
        "boto3_large_message_utils.builder.LargeMessageBuilder._get_compressed_message_body"
    )
    def test_compressed_message_is_returned_when_compress_is_true(
        self, mock_get_compressed_message_body
    ):
        mock_get_compressed_message_body.return_value = "<Compressed Message Body>"

        self.base.message_size_threshold = 50
        self.base.compress = True

        test_message = "This is a really long string. 56 characters to be exact."
        test_message_attributes = {
            "string": {
                "StringValue": "string",
                "BinaryValue": b"bytes",
                "DataType": "string",
            }
        }

        expected = (
            "<Compressed Message Body>",
            {
                "ORIGINAL_MESSAGE_SIZE": {"DataType": "Number", "StringValue": "56"},
                "string": {
                    "StringValue": "string",
                    "BinaryValue": b"bytes",
                    "DataType": "string",
                },
            },
        )

        actual = self.base._handle_message_with_message_attributes(
            test_message, test_message_attributes
        )

        self.assertEqual(expected, actual)

    @patch(
        "boto3_large_message_utils.builder.LargeMessageBuilder._store_message_in_s3",
        return_value='{"bucket": "test-s3-bucket", "key": "test-object-key", "compressed": false}',
    )
    def test_cached_message_is_returned_when_compress_is_false(
        self, mock_store_message_in_s3
    ):
        self.base.message_size_threshold = 40
        self.base.compress = False

        test_message = "This is a really long string. 56 characters to be exact."
        test_message_attributes = {
            "string": {
                "StringValue": "string",
                "BinaryValue": b"bytes",
                "DataType": "string",
            }
        }

        expected = (
            '{"bucket": "test-s3-bucket", "key": "test-object-key", "compressed": false}',
            {
                "ORIGINAL_MESSAGE_SIZE": {"DataType": "Number", "StringValue": "56"},
                "string": {
                    "StringValue": "string",
                    "BinaryValue": b"bytes",
                    "DataType": "string",
                },
            },
        )

        actual = self.base._handle_message_with_message_attributes(
            test_message, test_message_attributes
        )

        self.assertEqual(expected, actual)

    @patch(
        "boto3_large_message_utils.builder.LargeMessageBuilder._store_message_in_s3",
        return_value='{"bucket": "test-s3-bucket", "key": "test-object-key", "compressed": true}',
    )
    def test_cached_message_is_returned_when_compress_is_true(
        self, mock_store_message_in_s3
    ):
        self.base.message_size_threshold = 24
        self.base.compress = True

        test_message = "This is a really long string. 56 characters to be exact."
        test_message_attributes = {
            "string": {
                "StringValue": "string",
                "BinaryValue": b"bytes",
                "DataType": "string",
            }
        }

        expected = (
            '{"bucket": "test-s3-bucket", "key": "test-object-key", "compressed": true}',
            {
                "ORIGINAL_MESSAGE_SIZE": {"DataType": "Number", "StringValue": "56"},
                "string": {
                    "StringValue": "string",
                    "BinaryValue": b"bytes",
                    "DataType": "string",
                },
            },
        )
        actual = self.base._handle_message_with_message_attributes(
            test_message, test_message_attributes
        )

        self.assertEqual(expected, actual)

    # TODO: Add some exception handling tests


@patch(
    "boto3_large_message_utils.builder.generate_s3_object_key",
    return_value="abcde-fghi-jklm-nopqrstuvwxyz",
)
class TestStoreInS3(TestCase):
    def setUp(self):
        self.base = LargeMessageBuilder(s3_bucket_for_cache="test-s3-bucket")
        self.base.s3 = Mock()

    def test_put_object(self, mock_uuid):
        test_message = "this is a test message"

        self.base._store_message_in_s3(test_message)

        self.base.s3.put_object.assert_called_with(
            Bucket="test-s3-bucket",
            Body=b"this is a test message",
            Key="abcde-fghi-jklm-nopqrstuvwxyz",
        )

    @patch(
        "boto3_large_message_utils.builder.compress_string",
        return_value=b"test bytes",
    )
    def test_put_object_with_compression(self, mock_uuid, mock_compress_string):
        self.base.compress = True

        test_message = "this is a test message"

        self.base._store_message_in_s3(test_message)

        self.base.s3.put_object.assert_called_with(
            Bucket="test-s3-bucket",
            Body=b"test bytes",
            Key="abcde-fghi-jklm-nopqrstuvwxyz",
        )


@patch(
    "boto3_large_message_utils.builder.LargeMessageBuilder._handle_message_with_message_attributes"
)
@patch("boto3_large_message_utils.builder.LargeMessageBuilder._handle_message")
class TestSubmitMessage(TestCase):
    def setUp(self):
        self.base = LargeMessageBuilder(s3_bucket_for_cache="test-s3-bucket")
        self.base.s3 = Mock()

    def test_message_without_message_attributes(
        self, mock_handle_msg, mock_handle_msg_with_attrs
    ):
        test_message = "this is a test message"

        self.base.build(test_message)

        mock_handle_msg.assert_called_with(test_message)
        mock_handle_msg_with_attrs.assert_not_called()

    def test_message_with_message_attributes(
        self, mock_handle_msg, mock_handle_msg_with_attrs
    ):
        test_message = "this is a test message"
        test_attributes = {"msg": "attrs"}

        self.base.build(test_message, test_attributes)

        mock_handle_msg.assert_not_called()
        mock_handle_msg_with_attrs.assert_called_with(test_message, test_attributes)
