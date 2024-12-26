from typing import List, Tuple
import apache_beam as beam
import os
from google.cloud import pubsub_v1
from dotenv import load_dotenv
from apache_beam import window
from apache_beam.transforms.trigger import AccumulationMode, AfterCount, Repeatedly

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

    options = beam.options.pipeline_options.PipelineOptions()
    options.view_as(beam.options.pipeline_options.StandardOptions).streaming = True

    p = beam.Pipeline(options=options)
        
    @beam.typehints.with_input_types(List[str])
    @beam.typehints.with_output_types(window.TimestampedValue)
    def timestamp(element: List[str]) -> window.TimestampedValue:
        try:
            unix_timestamp = element[16].strip().replace("\\r", "").replace("\\n", "").replace("'", "")
            if not unix_timestamp.isdigit():
                raise ValueError(f"Invalid timestamp value: {unix_timestamp}")
            return window.TimestampedValue(element, int(unix_timestamp))
        except (IndexError, ValueError) as e:
            raise ValueError(f"Invalid timestamp at element {element}: {e}") from e

    @beam.typehints.with_input_types(str)
    @beam.typehints.with_output_types(Tuple[str,int])
    def extractPlayer(element: List[str]) -> Tuple[str, int]:
        return (element[1], 1)
    
    @beam.typehints.with_input_types(str)
    @beam.typehints.with_output_types(Tuple[str,int])
    def extractTeam(element: List[str]) -> Tuple[str, int]:
        return (element[3], 1)
    
    @beam.typehints.with_input_types(Tuple[str, int])
    @beam.typehints.with_output_types(str)
    def encode(element: Tuple[str, int]) -> str:
        player, score = element
        string =  player + ": " + str(score)
        return string.encode("utf-8")
    
    def decode(element):
        element = str(element.decode("utf-8"))
        element = element.rstrip().lstrip()
        element = element.rstrip('\\r\\n')
        return element

    pubsub_data = (
        p
        | 'Read from input sub' >> beam.io.gcp.pubsub.ReadFromPubSub(subscription = input_subscription_path)
        | 'Decode' >> beam.Map(decode)
        | 'Split' >> beam.Map(lambda element: element.split(','))
        | 'Custom timestamp' >> beam.Map(timestamp)
    )

    player_score = (
        pubsub_data
        | 'Extract player and score' >> beam.Map(extractPlayer)
        | 'Window player score' >> beam.WindowInto(window.GlobalWindows(), trigger=Repeatedly(AfterCount(2)), accumulation_mode=AccumulationMode.ACCUMULATING)
        | 'Sum' >> beam.CombinePerKey(sum)
        | 'Encode' >> beam.Map(encode)
        | 'Send to output topic' >> beam.io.gcp.pubsub.WriteToPubSub(output_topic_path)
    )

    team_score = (
        pubsub_data
        | 'Extract team and score' >> beam.Map(extractTeam)
        | 'Window team score' >> beam.WindowInto(window.GlobalWindows(), trigger=Repeatedly(AfterCount(2)), accumulation_mode=AccumulationMode.ACCUMULATING)
        | 'Sum for team' >> beam.CombinePerKey(sum)
        | 'Encode team' >> beam.Map(encode)
        | 'Send to output topic team' >> beam.io.gcp.pubsub.WriteToPubSub(output_topic_path)
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