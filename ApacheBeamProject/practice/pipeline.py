import apache_beam as beam

def parse_and_split(record):
    return record.split(',')

def filter_accounts(record):
    return record[3] == 'Accounts'

def parse_and_count(record):
    return (record[1], 1)


with beam.Pipeline() as p1:
    input_collection = (
        p1
        | 'Read from file' >> beam.io.ReadFromText('./data/dept_data.txt')
        | 'Parse and split' >> beam.Map(parse_and_split)
    )

    attendance_count = (
        input_collection
        | 'Filter Accounts' >> beam.Filter(filter_accounts)
        | 'Map (name, 1)' >> beam.Map(parse_and_count)
        | beam.CombinePerKey(sum)
        | beam.Map(lambda employee_count: str(employee_count))
        | beam.io.WriteToText('data/output')
    )