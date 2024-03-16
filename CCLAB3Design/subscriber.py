import os
from google.cloud import pubsub_v1
import json

# Set Google Cloud credentials and project details
credPath = os.path.abspath('iron-ring-417318-9957ad4d30a1.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credPath
project_id = "iron-ring-417318"
subscription_id = "smartMeter-filtered-sub"

consumer = pubsub_v1.SubscriberClient()
subscription_path = consumer.subscription_path(project_id, subscription_id)

def callback(message):
    print(f"Received {json.loads(message.data)}.")
    message.ack

streaming_pull_future = consumer.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

try:
    while True:
        pass
except KeyboardInterrupt:
    print("Stop Subscriber")
finally:
    consumer.close()
