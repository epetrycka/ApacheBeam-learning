import time
from google.cloud import pubsub_v1
from dotenv import load_dotenv
import os

if __name__ == "__main__":
    load_dotenv('./.env')

    store_data = "../data/mobile_game.txt"

    publisher = pubsub_v1.PublisherClient()

    project_id = os.getenv("PROJECT_ID")
    topic_id = os.getenv("TOPIC_ID")
    topic_path = publisher.topic_path(project=project_id, topic=topic_id)

    try:
        publisher.create_topic(request={"name":topic_path})
        print(f"Topic {topic_id} has been created")
    except Exception as e:
        print(f"Topic {topic_id} already exist: {e}")

    with open(store_data, 'rb') as file:
        for line in file:
            data = line.encode("utf-8")
            future = publisher.publish(topic_path, data)
            print(future.result(), f": published {data} to {topic_id}")
            time.sleep(2)