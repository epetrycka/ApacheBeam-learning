import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse

parser = argparse.ArgumentParser() 
	
parser.add_argument('--input',           
                      dest='input',
                      required=True,
                      help='Input file to process.')
parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')

path_args, pipeline_args = parser.parse_known_args()   

inputs_pattern = path_args.input   
outputs_prefix = path_args.output 


options = PipelineOptions(pipeline_args + ['--region=us-central1'])
p = beam.Pipeline(options=options)

attendance_count =   (
                          p
                          | 'Read lines' >> beam.io.ReadFromText(inputs_pattern)
                          | 'Split row' >> beam.Map(lambda record: record.split(','))
                          | 'Get all Accounts Dept Persons' >> beam.Filter(lambda record: record[3] == 'Accounts')
                          | 'Pair each employee with 1' >> beam.Map(lambda record: (record[1], 1))
                          | 'Group and sum' >> beam.CombinePerKey(sum)
                          | 'Format results' >> beam.Map(lambda employee_count: str(employee_count))
                          | 'Write results' >> beam.io.WriteToText(outputs_prefix)
                      )

p.run()