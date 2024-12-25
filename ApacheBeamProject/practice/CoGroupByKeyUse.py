import apache_beam as beam

def split(record: str) -> tuple[str, list[str]]:
    splitted = record.split(',')
    return (splitted[0], splitted[1:])

def extract_sec_atribute(record: tuple[str, list[str]]) -> tuple[str, str]:
    (key, value) = record
    return (key, value[1])

def extract_first_atribute(record: tuple[str, list[str]]) -> tuple[str, str]:
    (key, value) = record
    return (key, value[0])

with beam.Pipeline() as p:
    input_dept = (
        p
        | 'Read from file dept_data' >> beam.io.ReadFromText('./data/dept_data.txt')
        | 'Split to dictionary' >> beam.Map(split)
        | 'Extract city' >> beam.Map(extract_first_atribute)
        | 'Remove duplicates cities' >> beam.Distinct()
    )

    input_loc = (
        p
        | 'Read from file loc_data' >> beam.io.ReadFromText('./data/location.txt')
        | 'Split loc to dictonary'>> beam.Map(split)
        | 'Extract name' >> beam.Map(extract_sec_atribute)
        | 'Remove duplicates name' >> beam.Distinct()
    )

    result = (
        {input_loc, input_dept}
        | 'Join' >> beam.CoGroupByKey()
        | 'Write result' >> beam.io.WriteToText('./data/cogroup.txt')
    )
