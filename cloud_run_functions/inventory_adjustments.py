import pandas as pd
import random
import string
import datetime
import io
import functions_framework
from google.cloud import storage

def generate_random_records_df(number_of_records=random.randint(1000,10000)):

    item_id_list = [ ''.join([random.choice(string.ascii_letters) for i in range(5)]).lower() + str(random.randint(100,999)) for i in range(number_of_records) ]
    item_name_list = [ random.choice([f'Item_{i}' for i in range(1,20)]) for i in range(number_of_records) ]
    quantity_change_list = [ random.randint(1,100) for i in range(number_of_records)  ]
    current_stock_list = [random.randint(1,10000) for i in range(number_of_records)  ]
    location_list = [ random.choice([f'Location_{i}' for i in range(1,20)]) for i in range(number_of_records) ]
    event_time_list = [ datetime.date.today().isoformat() for i in range(number_of_records) ]


    records_dict = {'item_id': item_id_list, 
                    'item_name': item_name_list, 
                    'quantity_change':quantity_change_list,
                    'current_stock': current_stock_list,
                    'location': location_list,
                    'event_time': event_time_list,
                    }
    
    records_df = pd.DataFrame(records_dict)

    return records_df

def send_data_gcs(data, bucket_name='inventory_bucker', file_name=f'inventory_adjustments'):
    gcs_uri = f'gs://{bucket_name}/{file_name}.csv'
    
    try:
        data.to_csv(gcs_uri,index=False) 
        print(f"DataFrame successfully uploaded to: {gcs_uri}")
        return True
    except Exception as e:
        print(f"Error uploading DataFrame to GCS: {e}")
        return False

def main(request):
    data = generate_random_records_df()
    send_data_gcs(data)
    return 'Inventroy Adj. Data succesfully sent to GCS', 200




# 1) See if data exists in Cloud Storage if not create a new data frame and Store a CSV in Cloud storage
# bucket_name = 'inventory_bucker'
# file_path = 'my_data.csv'
# storage_client = storage.Client()
# bucket = storage_client.bucket(bucket_name)
# blob = bucket.blob(file_path)

# try:

#     if blob.exists():
#         print(f"File '{file_path}' exists in bucket '{bucket_name}'. Reading CSV...")
#         csv_data = blob.download_as_bytes()
#         df = pd.read_csv(io.BytesIO(csv_data))
#     else:
#         print(f"File '{file_path}' does not exist in bucket '{bucket_name}'.")

# except Exception as e:
#     print(f"An error occurred: {e}")


# 2) Simlulate new data entrances to the CSV file

# random_record =  {
#     'item_id': ''.join([random.choice(string.ascii_letters) for i in range(5)]) + str(random.randint(1,1000)),
#     'item_name': random.choice([f'Item_{i}' for i in range(1,20)]),
#     'quantity_change': random.randint(1,100),
#     'current_stock': random.randint(1,10000),
#     'location': random.choice([f'Location_{i}' for i in range(1,20)]),
#     'event_time': datetime.date.today().isoformat()
# }

# inventory_adjustments_df = pd.DataFrame(columns=random_record.keys())
# inventory_adjustments_df



# from google.cloud import storage
# import pandas as pd

# def check_and_read_gcs_csv(bucket_name, file_path):
#     try:
#         # Initialize the GCS client
#         storage_client = storage.Client()

#         # Get the bucket object
#         bucket = storage_client.bucket(bucket_name)

#         # Get the blob (file) object
#         blob = bucket.blob(file_path)

#         # Check if the blob exists
#         if blob.exists():
#             print(f"File '{file_path}' exists in bucket '{bucket_name}'. Reading CSV...")
#             # Download the CSV file as bytes and read it into a Pandas DataFrame
#             csv_data = blob.download_as_bytes()
#             df = pd.read_csv(io.BytesIO(csv_data))
#             return df
#         else:
#             print(f"File '{file_path}' does not exist in bucket '{bucket_name}'.")
#             return None

#     except Exception as e:
#         print(f"An error occurred: {e}")
#         return None

# if __name__ == "__main__":
#     # Replace with your actual bucket name and file path
#     your_bucket_name = "your-gcs-bucket-name"
#     your_file_path = "path/to/your_data.csv"

#     # Check and read the CSV file
#     data_frame = check_and_read_gcs_csv(your_bucket_name, your_file_path)

#     # Print the DataFrame if it was successfully read
#     if data_frame is not None:
#         print("\nDataFrame from GCS:")
#         print(data_frame)