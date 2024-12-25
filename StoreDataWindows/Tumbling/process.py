from typing import List
import apache_beam as beam
import os
from google.cloud import pubsub_v1
from dotenv import load_dotenv
from apache_beam import window
from apache_beam.transforms.trigger import AccumulationMode, AfterWatermark, AfterProcessingTime, AfterCount, Repeatedly

if __name__ == "__main__":
    if True:
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
            print(f"Subscription '{input_subscription_id}' has been created.")
        except Exception as e:
            print(f"Subscription {input_subscription_id} already exist: {e}")

        output_topic_id = os.getenv("OUTPUT_TOPI_ID")
        output_topic_path = publisher.topic_path(project_id, output_topic_id)

        try:
            publisher.create_topic(request={"name": output_topic_path})
            print(f"Topic '{output_topic_id}' has been created.")
        except Exception as e:
            print(f"Topic {output_topic_id} already exist: {e}")

    @beam.typehints.with_input_types(List[str])
    @beam.typehints.with_output_types(List[str])
    def profit(element: List[str]) -> List[str]:
        amount = element[4]
        buy_rate = element[5]
        sell_price = element[6]
        profit = (int(sell_price) - int(buy_rate)) * int(amount)
        element.append(str(profit))
        return element

    options = beam.options.pipeline_options.PipelineOptions()
    options.view_as(beam.options.pipeline_options.StandardOptions).streaming = True

    p = beam.Pipeline(options=options)
        
    pubsub_data = (
        p
        | 'Read from input sub' >> beam.io.gcp.pubsub.ReadFromPubSub(subscription = input_subscription_path)
        | 'Decode' >> beam.Map(lambda element: element.decode("utf-8").rstrip().lstrip())
        | 'Not None' >> beam.Filter(lambda element: element is not None)
        | 'Map to list' >> beam.Map(lambda element: element.split(','))
        | 'Filter by country' >> beam.Filter(lambda element: element[1] == 'Mumbai' or element[1] == 'Bangalore')
        | 'Create profit column' >> beam.Map(profit)
        #| 'Debugging output' >> beam.Map(lambda element: print(type(element[0]), type(int(element[7]))))
        | 'Create dict form' >> beam.Map(lambda element: (element[0], int(element[7])) )
        | 'Window' >> beam.WindowInto(window.GlobalWindows(), trigger=Repeatedly(AfterCount(5)), accumulation_mode=AccumulationMode.DISCARDING)
        | 'Sum values' >> beam.CombinePerKey(sum)
        | 'Encode result' >> beam.Map(lambda element: str(element).encode("utf-8"))
        | 'Send to output topic' >> beam.io.gcp.pubsub.WriteToPubSub(output_topic_path)
    )

    try:
        result = p.run()
        result.wait_until_finish()
    except KeyboardInterrupt:
        print("Interrupted")
        result.cancel()
    finally:
        if result:
            result.cancel()