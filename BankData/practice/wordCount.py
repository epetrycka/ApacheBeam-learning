from typing import Iterable
import apache_beam as beam
import re

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

with beam.Pipeline() as p:
    input = (
        p
        | 'Read from file' >> beam.io.ReadFromText('./data/data.txt')
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
        | 'Write to file' >> beam.io.WriteToText('./data/word_counter')
    )
