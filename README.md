# boto3_large_message_utils

This library provides a way of bypassing AWS size restrictions when using services such as SQS and SNS, by providing methods to cache message bodies in S3 and parse them again at the other end.

## Usage

### Install

Install the package using pip

```shell script
pip install boto3_large_message_utils
```

### Initialise Handler

Import and set up the `LargeMessageHandler`

```python
from boto3_large_message_utils import LargeMessageHandler

msg_handler = LargeMessageHandler(
    s3_bucket_for_cache='my-bucket', #REQUIRED
    s3_object_prefix='my-prefix',
    compress=True,
    #message_size_threshold=100000, # Pass an optional message size threshold
    #session=session, # Pass an optional boto3 session to initialise the client from the session
)
```

### Handle a message

```python
# create your message in the normal way, submit_message expects a string
my_message = json.dumps({ 'content': 'this is my message' })

# submit your message to the handler
message = msg_handler.submit_message(my_message)
# send message to SQS, SNS or another AWS service
```

### Message with Message Attributes

```python
# create your message in the normal way, submit_message expects a string
my_message = json.dumps({ 'content': 'this is my message' })
message_attributes = {
    "MSG_ATTR": {
        "StringValue": "my-value"
    }
}

# submit your message to the handler
message = msg_handler.submit_message(my_message, message_attributes)
# send message to SQS, SNS or another AWS service
```

### Parse a message

Handle a message that has been optimised by the LargeMessageHandler.

```python
# received message from SQS or another AWS service.
my_message = msg_handler.parse_message(received_message)
```
