from typing import List, Optional
import apache_beam as beam

class ProcessNames(beam.DoFn):
    def process(self, element: str, cutoff_length: int, marker: str) -> Optional[List[str]]:
        name = element.split(',')[1]
        if len(name) <= cutoff_length:
            yield beam.pvalue.TaggedOutput('Short_Names', name)
        else:
            yield beam.pvalue.TaggedOutput('Long_Names', name)  
        if name.startswith(marker):
                yield name


with beam.Pipeline() as p:
    input = (
        p
        | 'Read from file dept_data.txt' >> beam.io.ReadFromText('./data/dept_data.txt')
        | 'Split to outputs' >> beam.ParDo(ProcessNames(), cutoff_length=4, marker='A').with_outputs('Long_Names', 'Short_Names', main='StartsWith_A')
    )

    input['Short_Names'] | '1' >> beam.Distinct() | 'Write Short Names' >> beam.io.WriteToText('./data/short_names.txt') 
    input['Long_Names'] | '2' >> beam.Distinct() | 'Write Long Names' >> beam.io.WriteToText('./data/long_names.txt') 
    input['StartsWith_A'] | '3' >> beam.Distinct() | 'Write A Names' >> beam.io.WriteToText('./data/with_A.txt') 