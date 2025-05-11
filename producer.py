from google.api_core.exceptions import NotFound
from google.cloud.pubsub import PublisherClient, SchemaServiceClient
from google.pubsub_v1.types.schema import Schema, Encoding, GetSchemaRequest, SchemaView
from google.protobuf.json_format import MessageToJson
from google.pubsub_v1.types import Encoding
from utilities import us_states_pb2
from utilities import inventory_event_pb2



PROJECT_ID  = "real-time-data-pipeline-457520"
TOPIC_ID = "inventory-topic"

publisher_client = PublisherClient()
schema_client = SchemaServiceClient()

topic_path = publisher_client.topic_path(project = PROJECT_ID, topic = TOPIC_ID)
topic = publisher_client.get_topic(request={"topic": topic_path})
# Get the schema name from the topic
schema_name = topic.schema_settings.schema
# Create schema request to fetch full definition
request = GetSchemaRequest(name=schema_name, view=SchemaView.FULL)
# Fetch the schema
schema = schema_client.get_schema(request=request)
# Print the schema definition
# print(f"Schema name: {schema.name}")
# print("Schema definition:")
# print(schema.definition)

try:
    # Get the topic encoding type.
    topic = publisher_client.get_topic(request={"topic": topic_path})
    encoding = topic.schema_settings.encoding

    # Instantiate a protoc-generated class defined in `us-states.proto`.
    state = us_states_pb2.StateProto()
    state.name = "California"
    state.post_abbr = "CA"
    record = {'item_id': '123412',
              'item_name': 'widget',
              'quantity_change': 3,
              'current_stock': 10,
              'location': 'San Francisco',
              'event_time': "2025-05-03T13:00:00Z"}
    
    event = inventory_event_pb2.InventoryEvent()
    event.item_id = record['item_id']
    event.item_name = record['item_name']
    event.quantity_change = record['quantity_change']
    event.current_stock = record['current_stock']
    event.location = record['location']
    event.event_time = record['event_time']
    
    # Encode the data according to the message serialization type.
    if encoding == Encoding.BINARY:
        data = event.SerializeToString()
        print(f"Preparing a binary-encoded message:\n{data}")
    elif encoding == Encoding.JSON:
        json_object = MessageToJson(event)
        data = str(json_object).encode("utf-8")
        print(f"Preparing a JSON-encoded message:\n{data}")
    else:
        print(f"No encoding specified in {topic_path}. Abort.")
        exit(0)

    future = publisher_client.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

except NotFound:
    print(f"{TOPIC_ID} not found.")