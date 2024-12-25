from google.cloud import pubsub_v1
import time
from dotenv import load_dotenv
import os

if __name__ == "__main__":
    load_dotenv("./.env")

    project_id = os.getenv("PROJECT_ID")
    topic_id = os.getenv("TOPIC_ID")

    publisher = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(project_id, topic_id)

    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"Topic '{topic_id}' został utworzony.")
    except Exception as e:
        print(f"Topic już istnieje: {e}")

    file_path = "./counts.csv"

    with open(file_path) as file:
        header = file.readline()

        for line in file:
            line = int(line)
            data_str = f"Send number: {line}"
            attribute = "True" if line % 2 == 0 else "False"
            data = data_str.encode("utf-8")
            future = publisher.publish(topic_path, data, even=attribute)
            print(future.result(), f", attributes={attribute}")
            time.sleep(1)

    print(f"Published messages to {topic_path}")