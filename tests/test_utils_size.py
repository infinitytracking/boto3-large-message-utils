from unittest import TestCase
from unittest.mock import patch

from boto3_large_message_utils.utils.size import (
    get_size_of_string_in_bytes
)


class TestGetSizeOfStringInBytes(TestCase):
    def test_correct_string_length_is_returned(self):
        expected = 30
        actual = get_size_of_string_in_bytes("This string has 30 characters!")

        self.assertEqual(expected, actual)

    def test_value_error_is_raised(self):
        with self.assertRaises(ValueError):
            get_size_of_string_in_bytes({"msg": "this method only supports strings"})