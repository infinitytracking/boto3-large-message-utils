# boto3_large_message_utils

This library provides a way of bypassing AWS size restrictions when using services such as SQS and SNS, by providing methods to cache message bodies in S3 and parse them again at the other end.

## Usage

### Install

Install the package using pip

```shell script
pip install boto3_large_message_utils
```

### Initialise Handler

Import and set up the `LargeMessageBuilder`

```python
from boto3_large_message_utils import LargeMessageBuilder

builder = LargeMessageBuilder(
    s3_bucket_for_cache='my-bucket', #REQUIRED
    s3_object_prefix='my-prefix',
    compress=True,
    #message_size_threshold=100000, # Pass an optional message size threshold
    #session=session, # Pass an optional boto3 session to initialise the client from the session
)
```

### Handle a message

```python
# create your message in the normal way, build expects a string
msg = json.dumps({ 'content': 'this is my message' })

# submit your message to the handler
new_msg = builder.build(msg)
# send message to SQS, SNS or another AWS service
```

### Message with Message Attributes

```python
# create your message in the normal way, build expects a string
msg = json.dumps({ 'content': 'this is my message' })
msg_attr = {
    "MSG_ATTR": {
        "StringValue": "my-value"
    }
}

# submit your message to the handler
msg = builder.build(msg, msg_attr)
# send message to SQS, SNS or another AWS service
```

### Parse a message

Handle a message that has been optimised by the Base.

```python
# received message from SQS or another AWS service.
parser = LargeMessageParser(
    #session=session, # Pass an optional boto3 session to initialise the client from the session
)
msg = parser.parse(received_message)
```
