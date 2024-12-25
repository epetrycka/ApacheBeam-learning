from typing import Iterable, List, Optional, Tuple
import apache_beam as beam

exclude_ids = list()
with open ('./data/exclude_ids.txt') as file:
    exclude_ids = [str(line.strip()) for line in file.readlines()]

def split(record: str) -> List[str]:
    return record.rstrip().split(',')

def filter_account(record: List[str]) -> bool:
    return record[3] == 'Accounts'

def create_dict(record: List[str]) -> Tuple[str, Tuple[str, int]]:
    return (record[0], (record[1], 1))

def sum_counts(record: Iterable[Tuple[str, int]]) -> Tuple[str, int]:
    names, counts = zip(*record)
    return (str(names[0]), int(sum(counts)))

class FilterExcludeIds(beam.DoFn):
    def process(self, element: Tuple[str, Tuple[str, int]], ids: List[str]) -> Optional[Iterable[Tuple[str, Iterable[Tuple[str, int]]]]]:
        id = element[0]
        if any(idx != id for idx in ids):
            return [element]
            #hihi bug I cannot solve it :D

class FilterNamesByLength(beam.DoFn):
    def process(self, element: Tuple[str, Iterable[Tuple[str, int]]], lower_bound: int, upper_bound: int=float('inf')) -> Optional[Tuple[str, Iterable[Tuple[str, int]]]]:
        name = element[1][0][0]
        print(len(name))
        if (lower_bound <= len(name) <= upper_bound):
            return element
    
with beam.Pipeline() as p:
    input = (
        p
        | 'Read the dept_data.txt file' >> beam.io.ReadFromText('./data/dept_data.txt')
        | 'Parse and split' >> beam.Map(split)
        | 'Filter accounts' >> beam.Filter(filter_account)
        | 'Create dict (key, (name, count))' >> beam.Map(create_dict)
        | 'Group and count' >> beam.CombinePerKey(sum_counts)
    )

    output1 = (
        input 
        | 'Write to file output temp' >> beam.io.WriteToText('./data/temp_side_input.txt')
    )

    exclue_ids_pvalue = p | 'Create exclude ids'>> beam.Create(exclude_ids)
    
    output2 = (
        input
        | 'Filter ids not excluded' >> beam.ParDo(FilterExcludeIds(), ids=beam.pvalue.AsList(exclue_ids_pvalue))
        | 'Filter names length between 3 and 10' >> beam.ParDo(FilterNamesByLength(), lower_bound=3, upper_bound=10)
        | 'Write to file output' >> beam.io.WriteToText('./data/side_input.txt')
    )