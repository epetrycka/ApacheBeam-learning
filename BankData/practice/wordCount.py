from typing import Iterable
import apache_beam as beam
import re
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

parser = argparse.ArgumentParser()

parser.add_argument('--input',
                    dest='input',
                    required=True,
                    help='Input file to process')

parser.add_argument('--output',
                    dest='output',
                    required=True,
                    help='Output file to write results to')

path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input
output_pattern = path_args.output

# def seperate_words(record):
#     return re.split(r'[\[\](),:.\s]+', record)

def seperate_words(record):
    return re.findall(r'[A-Z,a-z]+', record)

def map_to_num(record):
    return (record, 1)

class SumCounts(beam.DoFn):
    def process(self, element: tuple[str, Iterable[int]]) -> tuple[str, int]:
        (key, values) = element
        return [(key,sum(values))]
    
def map_to_type(record):
    return type(record[0])

options = PipelineOptions(pipeline_args)

with beam.Pipeline(options=options) as p:
    input = (
        p
        | 'Read from file' >> beam.io.ReadFromText(inputs_pattern)
    )

    word_count = (
        input
        | 'Seperate words' >> beam.FlatMap(seperate_words)
        | 'Map to number' >> beam.Map(map_to_num)
        #| 'Map to the type' >> beam.Map(map_to_type)
        | 'Group By Key' >> beam.GroupByKey()
        | 'Count words' >> beam.ParDo(SumCounts())
        | 'Filter words abouve 1 occurrence' >> beam.Filter(lambda x: x[1] > 5)
    )

    output = (
        word_count
        | 'Write to file' >> beam.io.WriteToText(output_pattern)
    )

#input = ./data/data.txt
#output = ./data/word_counter