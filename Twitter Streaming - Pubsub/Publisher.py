from google.cloud import pubsub_v1
import json, pytz
from datetime import datetime
import time

# Configure the connection
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("Hazem-data-engineer", "twitter-egypt")


# Function to write data to
def write_to_pubsub(data):
    try:
        data = json.dumps({
            'uniqueID': data["id"],
            'text': data["text"],
            "user_name": data["user"]["screen_name"],
            "created_at": time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(data['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
        },ensure_ascii=False)

        # publish to the topic, don't forget to encode everything at utf8!
        publisher.publish(topic_path,
                          data.encode("utf-8"))
        print(data)

    except Exception as e:
        print(e)
        raise