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
        print(project_id)

        publisher = pubsub_v1.PublisherClient()

        input_topic_id = os.getenv("TOPIC_ID")
        input_topic_path = publisher.topic_path(project_id, input_topic_id)

        subscriber = pubsub_v1.SubscriberClient()

        input_subscription_id = os.getenv("INPUT_SUB_ID")
        input_subscription_path = subscriber.subscription_path(project_id, input_subscription_id)

        # try:
        #     subscriber.create_subscription(
        #         request={
        #             "name": input_subscription_path,
        #             "topic": input_topic_path,
        #         }
        #     )
        #     print(f"Subscription '{input_subscription_id}' has been created.")
        # except Exception as e:
        #     print(f"Subscription {input_subscription_id} already exist: {e}")

        output_topic_id = os.getenv("OUTPUT_TOPI_ID")
        output_topic_path = publisher.topic_path(project_id, output_topic_id)

        # try:
        #     publisher.create_topic(request={"name": output_topic_path})
        #     print(f"Topic '{output_topic_id}' has been created.")
        # except Exception as e:
        #     print(f"Topic {output_topic_id} already exist: {e}")

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
    def extractWeapon(element: List[str]) -> Tuple[str, int]:
        try:
            game_id = element[0].strip()
            player_id = element[1].strip()
            weapon = element[5].strip()
            battle_time = int(element[15].strip())
            player_weapon_rank = int(element[6].strip())
            prey_weapon_rank = int(element[13].strip())
            player_location = element[7].strip()
            prey_location = element[14].strip()

            total_points = 0
            diff = player_weapon_rank - prey_weapon_rank

            if 10 <= battle_time <= 20:
                total_points += 4
            if 20 < battle_time <= 30:
                total_points += 3
            if 30 < battle_time <= 40:
                total_points += 2
            if 40 < battle_time:
                total_points += 1

            if diff > 6:
                total_points += 3
            if 3 <= diff <= 6:
                total_points += 2
            else:
                total_points += 1

            if player_location != prey_location:
                total_points += 3

            return f"{game_id},{player_id},{weapon}", total_points
        except (IndexError, ValueError) as e:
            print(f"Error processing element {element}: {e}")
            return None
        
    @beam.typehints.with_input_types(Tuple[str, float])
    @beam.typehints.with_output_types(str)
    def encode(element: Tuple[str, float]) -> str:
        player, score = element
        string =  player + ": " + str(score)
        return string.encode("utf-8")
    
    @beam.typehints.with_input_types(bytes)
    @beam.typehints.with_output_types(str)
    def decode(element: bytes) -> str:
        element = str(element.decode("utf-8"))
        element = element.rstrip().lstrip()
        element = element.rstrip('\\r\\n')
        element = element.replace("'", "")
        return element
    
    class Average(beam.CombineFn):
        def create_accumulator(self) -> Tuple[float, int]:
            return (0.0, 0)
        
        def add_input(self, mutable_accumulator: Tuple[float, int], element: int) -> Tuple[float, int]:
            (sum_point , count) = mutable_accumulator
            return (sum_point + float(element), count + 1)

        def merge_accumulators(self, accumulators) -> Tuple[float, int]:
            sums, counts = zip(*accumulators)
            return (sum(sums), sum(counts))
        
        def extract_output(self, accumulator: Tuple[float, int]) -> float:
            sums, counts = accumulator
            return float(sums/counts) if counts else float('NaN')


    pubsub_data = (
        p
        | 'Read from input sub' >> beam.io.gcp.pubsub.ReadFromPubSub(subscription = input_subscription_path)
        | 'Decode' >> beam.Map(decode)
        | 'Split' >> beam.Map(lambda element: element.split(','))
        #| 'Custom timestamp' >> beam.Map(timestamp)
    )

    player_skilled_weapon = (
        pubsub_data
        | 'Extract player and weapon' >> beam.Map(extractWeapon)  
        | 'Window player score' >> beam.WindowInto(window.Sessions(5))
        | 'Sum' >> beam.CombinePerKey(Average())
        | 'Encode' >> beam.Map(encode)
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