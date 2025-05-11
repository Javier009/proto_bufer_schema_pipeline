import io
import pandas as pd

from google.api_core.exceptions import NotFound
from google.cloud.pubsub import PublisherClient, SchemaServiceClient
from google.pubsub_v1.types.schema import Schema, Encoding, GetSchemaRequest, SchemaView
from google.protobuf.json_format import MessageToJson
from google.pubsub_v1.types import Encoding
from google.cloud import storage
# from utilities import us_states_pb2
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

# Fetch data from inventroy adjustments bucket(r) -- > TYPO
bucket_name = 'inventory_bucker'
file_path = 'inventory_adjustments.csv'
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(file_path)
print(blob)

try:
    if blob.exists():
        print(f"File '{file_path}' exists in bucket '{bucket_name}'. Reading CSV...")
        csv_data = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(csv_data))
    else:
        print(f"File '{file_path}' does not exist in bucket '{bucket_name}'.")
        df = pd.DataFrame()

except Exception as e:
    print(f"An error occurred: {e}")


topic = publisher_client.get_topic(request={"topic": topic_path})
# Get the topic encoding type.
encoding = topic.schema_settings.encoding 

if df.empty == False:

    # for n in range(0,len(df)):
    for n in range(0,5):

        record = df.iloc[n].to_dict()

        try:
            # Events registration from each record
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
else:
    raise ValueError('Data Frame is empty or not exists, please check')