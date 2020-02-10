import json
from unittest import TestCase

import boto3
from botocore.exceptions import ClientError
from moto import mock_s3

from boto3_large_message_utils.large_message_handler import LargeMessageHandler

BUCKET_NAME = "my-test-bucket"
BUCKET_PREFIX = "my-bucket-prefix"


@mock_s3
class TestS3Integration(TestCase):
    def setUp(self):
        self.base = LargeMessageHandler(
            s3_bucket_for_cache=BUCKET_NAME, s3_object_prefix=BUCKET_PREFIX
        )

        self.client = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )
        try:
            self.resource = boto3.resource(
                "s3",
                region_name="eu-west-1",
                aws_access_key_id="fake_access_key",
                aws_secret_access_key="fake_secret_key",
            )

            self.resource.meta.client.head_bucket(Bucket=BUCKET_NAME)
        except ClientError:
            pass
        else:
            err = "{bucket} should not exist.".format(bucket=BUCKET_NAME)
            raise EnvironmentError(err)
        self.client.create_bucket(Bucket=BUCKET_NAME)

    def tearDown(self):
        bucket = self.resource.Bucket(BUCKET_NAME)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()

    def test_store_message_in_s3(self):
        test_message = "This is a test message"

        cached_message = json.loads(self.base._store_message_in_s3(test_message))

        keys = list(
            self.resource.Bucket(BUCKET_NAME).objects.filter(
                Prefix=cached_message.get("key")
            )
        )
        self.assertEqual(1, len(keys))

        s3_object = keys[0].get()
        s3_object_body = s3_object["Body"].read()
        s3_object_body = s3_object_body.decode()

        self.assertEqual(test_message, s3_object_body)

    def test_store_message_in_s3_exception_handling(self):
        self.base.s3_bucket_for_cache = "bucket-does-not-exist"

        test_message = "This is a test message"

        with self.assertRaises(ClientError):
            self.base._store_message_in_s3(test_message)

    def test_retrieve_message_from_s3(self):
        test_message = "This is a test message"
        test_key = "test-key"

        self.client.put_object(
            Bucket=BUCKET_NAME, Body=test_message.encode("utf-8"), Key=test_key
        )

        message = self.base._retrieve_message_from_s3(BUCKET_NAME, test_key)

        self.assertEqual(test_message, message)

    def test_retrieve_message_from_s3_exception_handling(self):
        test_key = "test-key-does-not-exist"

        with self.assertRaises(ClientError):
            self.base._retrieve_message_from_s3(BUCKET_NAME, test_key)
