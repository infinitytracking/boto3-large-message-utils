from unittest import TestCase
from unittest.mock import patch

from boto3_large_message_utils.utils.s3 import generate_s3_object_key


class TestGenerateS3ObjectKey(TestCase):
    @patch(
        "boto3_large_message_utils.utils.s3.uuid.uuid4",
        return_value="abcde-fghi-jklm-nopqrstuvwxyz",
    )
    def test_prefix_is_added(self, mock_uuid):
        expected = "my-test-prefix/abcde-fghi-jklm-nopqrstuvwxyz"
        actual = generate_s3_object_key(prefix="my-test-prefix")

        self.assertEqual(expected, actual)

    @patch(
        "boto3_large_message_utils.utils.s3.uuid.uuid4",
        return_value="abcde-fghi-jklm-nopqrstuvwxyz",
    )
    def test_prefix_gets_preceding_and_trailing_slashes_removed(self, mock_uuid):
        expected = "my-test-prefix-two/abcde-fghi-jklm-nopqrstuvwxyz"
        actual = generate_s3_object_key(prefix="///my-test-prefix-two///")

        self.assertEqual(expected, actual)

    @patch(
        "boto3_large_message_utils.utils.s3.uuid.uuid4",
        return_value="abcde-fghi-jklm-nopqrstuvwxyz",
    )
    def test_prefix_none(self, mock_uuid):
        expected = "abcde-fghi-jklm-nopqrstuvwxyz"
        actual = generate_s3_object_key()

        self.assertEqual(expected, actual)

    @patch(
        "boto3_large_message_utils.utils.s3.uuid.uuid4",
        return_value="abcde-fghi-jklm-nopqrstuvwxyz",
    )
    def test_prefix_empty_string(self, mock_uuid):
        expected = "abcde-fghi-jklm-nopqrstuvwxyz"
        actual = generate_s3_object_key(prefix="")

        self.assertEqual(expected, actual)
