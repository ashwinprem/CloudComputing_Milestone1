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
subscription_name = "design-sub";   # change it for your topic name if needed

# Initialize Pub/Sub Subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# Callback function to process messages
def callback(message):
    try:
        # Deserialize the message
        message_data = json.loads(message.data.decode("utf-8"))
        print(f"Received message: {message_data}")
        message.ack()  # Acknowledge the message
    except Exception as e:
        print(f"Failed to process message: {e}")
        message.nack()  # Negative acknowledgment if processing fails

# Listen for messages
print(f"Listening for messages on {subscription_path}...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
try:
    streaming_pull_future.result()  # Block the main thread to listen
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    streaming_pull_future.result()
