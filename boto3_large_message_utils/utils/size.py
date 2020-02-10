from boto3_large_message_utils.constants import RESERVED_ATTRIBUTE_NAME, MAX_ALLOWED_ATTRIBUTES
from boto3_large_message_utils.utils.compression import is_base64


def get_size_of_string_in_bytes(string: str) -> int:
    if isinstance(string, str):
        return len(string.encode("utf-8"))
    raise ValueError('"string" argument expects type "str"')


def get_message_attributes_size_in_bytes(message_attributes, message_size_threshold):
    if not isinstance(message_attributes, dict):
        raise ValueError('"message_attributes" argument expects type "dict"')

    message_attributes_size = 0

    for attribute_key, attribute_value in message_attributes.items():
        message_attributes_size += get_attribute_size(attribute_key, attribute_value)

    if message_attributes_size > message_size_threshold:
        raise ValueError(
            f"Total size of Message Attributes is {message_attributes_size} bytes which is larger than the "
            f"threshold of {message_size_threshold} bytes. Consider including the payload in the Message "
            f"Body instead of Message Attributes. "
        )

    return message_attributes_size


def get_attribute_size(attribute_key, attribute_value):
    attribute_size = get_size_of_string_in_bytes(attribute_key)
    if attribute_value.get("DataType"):
        attribute_size += get_size_of_string_in_bytes(attribute_value.get("DataType"))
    if attribute_value.get("StringValue"):
        attribute_size += get_size_of_string_in_bytes(attribute_value.get("StringValue"))
    if attribute_value.get("BinaryValue"):
        if is_base64(attribute_value.get("BinaryValue")):
            attribute_size += len(str.encode(attribute_value.get("BinaryValue")))
        else:
            binary_value = attribute_value.get("BinaryValue")
            if isinstance(binary_value, bytes):
                attribute_size += len(binary_value)
            else:
                attribute_size += get_size_of_string_in_bytes(binary_value)
    return attribute_size


def append_message_size_attribute(message_attributes, message_size):
    message_attributes_number = len(message_attributes)
    if message_attributes_number > MAX_ALLOWED_ATTRIBUTES:
        raise ValueError(
            f"Number of message attributes ({message_attributes_number}) exceeds the maximum allowed for "
            f"large-payload messages ({MAX_ALLOWED_ATTRIBUTES}). "
        )

    if message_attributes.get(RESERVED_ATTRIBUTE_NAME):
        raise ValueError(f"Message Attribute name {RESERVED_ATTRIBUTE_NAME} is reserved for use by "
                         f"MessageDispatchHelper.")

    message_attributes[RESERVED_ATTRIBUTE_NAME] = {
        "StringValue": str(message_size),
        "DataType": "Number",
    }

    return message_attributes
