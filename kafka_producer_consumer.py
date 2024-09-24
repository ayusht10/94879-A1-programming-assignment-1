import pickle
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import time
import gzip
import sys
import pandas as pd
from scipy.sparse import csr_matrix

# Load the Traffic Flow Forecasting Dataset from the .mat File
import scipy.io

# Function to load and prepare the data
def load_and_prepare_data(mat_file_path):
    mat = scipy.io.loadmat(mat_file_path)
    
    # Extract training and testing data
    tra_X_tr = mat['tra_X_tr']
    tra_Y_tr = mat['tra_Y_tr']
    tra_X_te = mat['tra_X_te']
    tra_Y_te = mat['tra_Y_te']
    tra_adj_mat = mat['tra_adj_mat']
    
    # Save the data using pickle for later use
    with open('tra_X_tr.pkl', 'wb') as f:
        pickle.dump(tra_X_tr, f)

    with open('tra_Y_tr.pkl', 'wb') as f:
        pickle.dump(tra_Y_tr, f)

    print("Data preparation complete. Data saved as pickle files.")

load_and_prepare_data('/Users/ayushtripathi/94879/traffic+flow+forecasting/traffic_dataset.mat')

### Python script to send traffic data as messages to a Kafka topic (Producer).
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Load the pickled data
with open('tra_X_tr.pkl', 'rb') as f:
    tra_X_tr = pickle.load(f)

with open('tra_Y_tr.pkl', 'rb') as f:
    tra_Y_tr = pickle.load(f)

# Convert to pandas DataFrames for easier handling
df_tra_X_tr = pd.DataFrame(tra_X_tr[0][0].toarray())  # Convert sparse matrix to dense
df_tra_Y_tr = pd.DataFrame(tra_Y_tr[:, 0])

# Define the chunk size
chunk_size = 10
num_locations = df_tra_Y_tr.shape[0]

def send_traffic_data():
    # Loop through each time step in the training data and send it to Kafka
    for i in range(df_tra_Y_tr.shape[1]):
        for start in range(0, num_locations, chunk_size):
            end = min(start + chunk_size, num_locations)
            # Example data payload for a subset of locations
            data = {
                'time_step': i,
                'location_data': df_tra_Y_tr.iloc[start:end, i].values.tolist(),  # Convert DataFrame slice to list
                'features': df_tra_X_tr.iloc[start:end, :].values.tolist()  # Convert DataFrame slice to list
            }
            # Compress the data before sending
            compressed_data = gzip.compress(json.dumps(data).encode('utf-8'))

            # Check the size of the compressed data
            size_of_message = sys.getsizeof(compressed_data)
            print(f"Size of compressed message at time step {i}, locations {start}-{end}: {size_of_message} bytes")

            # Ensure the size does not exceed 1 GB (or other appropriate limit)
            if size_of_message > 1073741824:  # 1 GB in bytes
                print("Warning: Message size exceeds 1 GB. Consider reducing chunk size or further compressing data.")
                continue
            
            # Send compressed data to Kafka topic 'traffic_data'
            producer.send('traffic_data', compressed_data)
            time.sleep(0.1)  # Adjust delay as needed for smaller chunks

if __name__ == "__main__":
    send_traffic_data()
    print("Data sending complete.")


### Python script to consume the traffic data from the Kafka topic (Consumer).

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'traffic_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Start reading at the earliest message in the topic
    enable_auto_commit=True,  # Automatically commit offsets
    group_id='traffic-consumer-group'  # Group ID for managing offsets
)


# Consume and process messages from Kafka topic
def consume_traffic_data():
    print("Starting to consume messages...")
    for message in consumer:
        try:
            # Attempt to decompress the message
            try:
                decompressed_data = gzip.decompress(message.value).decode('utf-8')
            except gzip.BadGzipFile:
                # If not gzipped, assume it's plain JSON
                decompressed_data = message.value.decode('utf-8')
            
            data = json.loads(decompressed_data)
            
            # Extract the data
            time_step = data['time_step']
            location_data = data['location_data']
            features = data['features']
            
            # Convert the received lists back to DataFrames for further processing if needed
            df_location_data = pd.DataFrame(location_data)
            df_features = pd.DataFrame(features)
            
            print(f"Received data for time step {time_step} with {df_location_data.shape[0]} locations")
            print(f"Location Data Head:\n{df_location_data.head()}")
            print(f"Features Data Head:\n{df_features.head()}")
            
        except Exception as e:
            print(f"Failed to process message: {e}")

if __name__ == "__main__":
    consume_traffic_data()
    print("Data consumption complete.")



'''
Citations:
producer.py
consumer.py
starter_noteboook_traffic_flow_prediction.py

* Code provided by the class.

'''