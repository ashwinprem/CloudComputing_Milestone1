import csv
import json
import os
import glob
from google.cloud import pubsub_v1

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set the project_id with your project ID
project_id="cobalt-broker-449201-a2";
topic_name = "design";   # change it for your topic name if needed
subscription_id = "design-sub";   # change it for your topic name if needed

# Initialize Pub/Sub Publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

# Read and publish CSV data
def publish_csv(file_path):
    with open(file_path, mode="r") as csv_file:
        reader = csv.DictReader(csv_file)  # Read CSV into a dictionary format
        for row in reader:
            # Serialize the row into JSON
            message = json.dumps(row).encode("utf-8")
            try:
                # Publish the message
                future = publisher.publish(topic_path, message)
                future.result()  # Wait for the publish to complete
                print(f"Published message: {row}")
            except Exception as e:
                print(f"Failed to publish message: {e}")

# Run the producer
csv_file_path = "Labels.csv"  # Replace with your actual CSV file path
publish_csv(csv_file_path)
