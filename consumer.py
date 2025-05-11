from concurrent.futures import TimeoutError
from google.cloud.pubsub import SubscriberClient
from google.protobuf.json_format import Parse
from google.cloud import pubsub_v1
# from utilities import us_states_pb2
from utilities import inventory_event_pb2


PROJECT_ID  = "real-time-data-pipeline-457520"
SUBSRIPTION_ID = "inventory-topic-sub"

# Number of seconds the subscriber listens for messages
timeout = 5.0

subscriber = SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSRIPTION_ID)

# Instantiate a protoc-generated class defined in `us-states.proto`.
# state = us_states_pb2.StateProto()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # Get the message serialization type.
    encoding = message.attributes.get("googclient_schemaencoding")
    event = inventory_event_pb2.InventoryEvent()
    record = message.data

    # Deserialize the message data accordingly.
    if encoding == "BINARY":
        event.ParseFromString(record)
        print(f"Received a binary-encoded message:\n{event}")
    elif encoding == "JSON":
        Parse(record, event)
        print(f"Received a JSON-encoded message:\n{event}")
    else:
        print(f"Received a message with no encoding:\n{message}")

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()  


