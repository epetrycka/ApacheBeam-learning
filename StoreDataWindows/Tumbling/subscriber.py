from google.cloud import pubsub_v1
from dotenv import load_dotenv
import os

if __name__ == "__main__":
    load_dotenv("./.env")
    timeout = 120.0

    project_id = os.getenv("PROJECT_ID")
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    topic_id = os.getenv("OUTPUT_TOPIC_ID")
    topic_path = publisher.topic_path(project=project_id, topic=topic_id)

    subscribe_id = os.getenv("OUTPUT_SUB_ID")
    subscribe_path = subscriber.subscription_path(project=project_id, subscription=subscribe_id)

    try:
        subscriber.create_subscription(request={
            "name":subscribe_path,
            "topic":topic_path
        })
        print(f"Subscription {subscribe_id} has been created")
    except Exception as e:
        print(f"Subscription {subscribe_id} already exsist: {e}")

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Received message: {message.data.decode('utf-8')}")
        message.ack()
    
    streaming_pull_future = subscriber.subscribe(subscription=subscribe_path, callback=callback)
    print("Listening...")

    with subscriber:
        try:
            print(streaming_pull_future.result(timeout=timeout))
        except TimeoutError:
            print("Timeout listening")
            streaming_pull_future.cancel()
            streaming_pull_future.result()
        except KeyboardInterrupt:
            print("Interrupted")
            streaming_pull_future.cancel()
            streaming_pull_future.result()
