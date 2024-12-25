from google.cloud import pubsub_v1
import os
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv("./.env")
    timeout = 5.0
    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()

    project_id = os.getenv("PROJECT_ID")

    output_topic_id = os.getenv("OUTPUT_TOPI_ID")
    output_topic_path = publisher.topic_path(project_id, output_topic_id)

    output_subscription_id = os.getenv("OUTPUT_SUB_ID")
    output_subscription_path = subscriber.subscription_path(project_id, output_subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        print(f"Otrzymano wiadomość: {message.data.decode('utf-8')}")
        message.ack()

    try:
        subscriber.create_subscription(
            request={
                "name": output_subscription_path,
                "topic": output_topic_path,
            }
        )
        print(f"Subskrypcja '{output_subscription_id}' została utworzona.")
    except Exception as e:
        print(f"Subskrypcja już istnieje: {e}")

    streaming_pull_future = subscriber.subscribe(output_subscription_path, callback=callback)
    print(f"Nasłuchiwanie")
    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            print("Odbieranie zakończone")
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result() # Block until the shutdown is complete.
    