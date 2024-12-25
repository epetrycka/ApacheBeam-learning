import os
import time 
from google.cloud import pubsub_v1

if __name__ == "__main__":

       # Replace  with your project id
    project = ''

    # Replace  with your pubsub topic
    pubsub_topic = ''

    # Replace with your service account path
    path_service_account = ''
	
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account    

    # Replace  with your input file path
    input_file = ''

    # create publisher
    publisher = pubsub_v1.PublisherClient()

    with open(input_file, 'rb') as ifp:
        # skip header
        header = ifp.readline()  
        
        # loop over each record
        for line in ifp:
            event_data = line   # entire line of input CSV is the message
            print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(1)    