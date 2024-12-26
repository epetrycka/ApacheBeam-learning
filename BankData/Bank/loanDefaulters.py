from typing import List, Tuple, Iterable
import apache_beam as beam
import datetime
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Command-line argument parsing
parser = argparse.ArgumentParser()
parser.add_argument('--input',
                    dest='input',
                    required=True,
                    help='Input file')
parser.add_argument('--output1',
                    dest='output1',
                    required=True,
                    help='Output file')
parser.add_argument('--output2',
                    dest='output2',
                    required=True,
                    help='Output file')

path_arg, pipeline_args = parser.parse_known_args()

input_pattern = path_arg.input
output_pattern1 = path_arg.output1
output_pattern2 = path_arg.output2

options = PipelineOptions(pipeline_args + ['--region=us-central1'])

# Helper function to split CSV rows
def split(record: str) -> List[str]:
    return record.split(',')

# Function to count late payments
def countLatePayments(record: List[str]) -> List[str]:
    try:
        # Parsing dates after stripping extra spaces
        due_data = datetime.datetime.strptime(record[6].strip(), '%d-%m-%Y')
        payment_data = datetime.datetime.strptime(record[8].strip(), '%d-%m-%Y')

        # Add flag for late payment (1 for late, 0 for on-time)
        record.append('1' if due_data < payment_data else '0')
    except ValueError as e:
        # Handle date parsing errors (optional logging)
        record.append('0')  # You might want to handle this differently
    return record

# Function to create a dictionary (Id + full name, missed payment count)
def makeDict(element: List[str]) -> Tuple[str, int]:
    return (element[0] + ', ' + element[1] + ' ' + element[2], int(element[9]))

# Function to format output
def output_format(element: Tuple[str, int]) -> str:
    name, missed = element
    return f"{name},{missed}"

# Function to show month of payment
def showMonthPayment(element: List[str]) -> Tuple[str, int]:
    try:
        payment_day = datetime.strptime(element[6].strip(), '%d-%m-%Y')
        return (element[0] + ', ' + str(element[1]) + ' ' + str(element[3]), payment_day.month)
    except ValueError as e:
        # Handle date parsing errors
        return (element[0] + ', ' + str(element[1]) + ' ' + str(element[3]), 0)

# Personal loan defaulters calculation
def personalLoanDef(element: Tuple[str, Iterable[int]]) -> Tuple[str, int]:
    name, payments = element
    total_missed = sum(payments)  # Total missed payments
    return (name, total_missed)

# Filter defaulters (3 or more missed payments)
def filterDefaulters(element: Tuple[str, int]) -> bool:
    _, total_missed = element
    return total_missed >= 3

# Pipeline execution
with beam.Pipeline(options=options) as p:
    input = (
        p
        | 'Read loan' >> beam.io.ReadFromText(input_pattern, skip_header_lines=1)
        | 'Split' >> beam.Map(split)
    )

    # Medical loan processing
    medical_def = (
        input
        | 'Filter medical loan' >> beam.Filter(lambda element: element[5] == 'Medical Loan')
        | 'Identify late payments' >> beam.Map(countLatePayments)
        | 'Map to (Id fullname, missed months)' >> beam.Map(makeDict)
        | 'Sum missed payments' >> beam.CombinePerKey(sum)
        | 'Filter defaulters' >> beam.Filter(lambda element: element[1] >= 3)
        | 'Output formatting' >> beam.Map(output_format)
        | 'Write to medical loan file' >> beam.io.WriteToText(output_pattern1)
    )

    # Personal loan processing
    personal_loan = (
        input
        | 'Filter personal loan' >> beam.Filter(lambda element: element[5] == 'Personal Loan')
        | 'Identify late payments and consecutive installments' >> beam.Map(showMonthPayment)
        | 'Group by key' >> beam.GroupByKey()
        | 'Calculate total missed payments' >> beam.Map(personalLoanDef)
        | 'Filter defaulters only' >> beam.Filter(filterDefaulters)
        | 'Output formatting 2' >> beam.Map(output_format)
        | 'Write to personal loan file' >> beam.io.WriteToText(output_pattern2)
    )
