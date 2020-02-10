import gzip
import io
from unittest import TestCase
from unittest.mock import Mock

from botocore.response import StreamingBody

from boto3_large_message_utils.parser import LargeMessageParser


class TestRetrieveFromS3(TestCase):
    def setUp(self):
        self.parser = LargeMessageParser()
        self.parser.s3.get_object = Mock()

    def test_retrieve_message_from_s3(self):
        test_message = "this is a mock message"
        test_key = "test-key"

        self.parser.s3.get_object.return_value = mock_s3_response(test_message.encode())

        actual = self.parser._retrieve_message_from_s3("test-s3-bucket", test_key)

        self.assertEqual(test_message, actual)
        self.parser.s3.get_object.assert_called_with(
            Bucket="test-s3-bucket", Key="test-key"
        )

    def test_retrieve_message_from_s3_with_compression(self):
        test_message = "this is a mock message"
        test_key = "test-key"

        self.parser.s3.get_object.return_value = mock_s3_response(
            gzip.compress(test_message.encode())
        )

        actual = self.parser._retrieve_message_from_s3("test-s3-bucket", test_key, True)

        self.assertEqual(test_message, actual)
        self.parser.s3.get_object.assert_called_with(
            Bucket="test-s3-bucket", Key="test-key"
        )


def mock_s3_response(body):
    return {"Body": StreamingBody(io.BytesIO(body), len(body))}
