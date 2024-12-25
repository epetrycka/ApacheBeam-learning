import json
from google.cloud import pubsub_v1
from dotenv import load_dotenv
import os
import apache_beam as beam

if __name__ == "__main__":
    load_dotenv("./.env")

    project_id = os.getenv("PROJECT_ID")
    publisher = pubsub_v1.PublisherClient()

    input_topic_id = os.getenv("TOPIC_ID")
    input_topic_path = publisher.topic_path(project_id, input_topic_id)

    subscriber = pubsub_v1.SubscriberClient()

    input_subscription_id = os.getenv("INPUT_SUB_ID")
    input_subscription_path = subscriber.subscription_path(project_id, input_subscription_id)

    try:
        subscriber.create_subscription(
            request={
                "name": input_subscription_path,
                "topic": input_topic_path,
            }
        )
        print(f"Subskrypcja '{input_subscription_id}' została utworzona.")
    except Exception as e:
        print(f"Subskrypcja już istnieje: {e}")

    output_topic_id = os.getenv("OUTPUT_TOPI_ID")
    output_topic_path = publisher.topic_path(project_id, output_topic_id)

    try:
        publisher.create_topic(request={"name": output_topic_path})
        print(f"Topic '{output_topic_id}' został utworzony.")
    except Exception as e:
        print(f"Topic już istnieje: {e}")

    options = beam.options.pipeline_options.PipelineOptions()
    options.view_as(beam.options.pipeline_options.StandardOptions).streaming = True

    p = beam.Pipeline(options=options)
        
    pubsub_data = (
        p
        | 'Read from input sub' >> beam.io.gcp.pubsub.ReadFromPubSub(subscription = input_subscription_path)
        | 'Send to output topic' >> beam.io.gcp.pubsub.WriteToPubSub(output_topic_path) 
    )

    try:
        result = p.run()
        result.wait_until_finish()
    finally:
        if result:
            result.cancel()