from typing import List, Tuple, Iterable
import apache_beam as beam
from time import strptime

def split(record: str) -> List[str]:
    return record.split(',')

def countLatePayments(record: List[str]) -> List[str]:
    due_data = strptime(record[6].rstrip().lstrip(), '%d-%m-%Y')
    payment_data = strptime(record[8].rstrip().lstrip(), '%d-%m-%Y')

    if due_data < payment_data:
        record.append('1')
    else:
        record.append('0')

    return record

def makeDict(element: List[str]) -> Tuple[str,int]:
    return ( element[0] + ', ' + element[1] + ' ' + element[2] , int(element[9]) )

def output_format(element: Tuple[str,int]) -> str:
    name, missed = element
    return str(name) + ',' + str(missed)

def showMonthPayment(element: List[str]) -> Tuple[str, int]:
    payment_day = strptime(element[6].rstrip().lstrip(), '%d-%m-%Y')
    return (element[0] + ', ' + str(element[1]) + ' ' + str(element[3]) , payment_day[1])

#not implemented:
def personalLoanDef(element: Tuple[str, Iterable[int]]) -> Tuple[str, int]:
    name, payments = element
    total_missed = sum(payments)  # Suma brakujących płatności
    return (name, total_missed)  # Zwracamy krotkę (str, int)

def filterDefaulters(element: Tuple[str, int]) -> bool:
    _, total_missed = element
    return total_missed >= 3

with beam.Pipeline() as p:
    input = (
        p
        | 'Read loan' >> beam.io.ReadFromText('./data/loan.txt', skip_header_lines=1)
        | 'Split' >> beam.Map(split)
    )

    medical_def = (
        input
        | 'Filter medical loan' >> beam.Filter(lambda element: element[5] == 'Medical Loan')
        | 'Identify late payments' >> beam.Map(countLatePayments)
        | 'Map to (Id fullname, missed months)' >> beam.Map(makeDict)
        | 'Sum missed payments' >> beam.CombinePerKey(sum)
        | 'Filter defaulters' >> beam.Filter(lambda element: element[1] >= 3)
        | 'Output formatting' >> beam.Map(output_format)
        | 'Write' >> beam.io.WriteToText('./data/medical_loan_def.txt')
    )

    personal_loan = (
        input
        | 'Filter personal loan' >> beam.Filter(lambda element: element[5] == 'Personal Loan')
        | 'Identify late payments and consecutive installments' >> beam.Map(showMonthPayment)
        | 'Group by key' >> beam.GroupByKey()
        | 'Calculate total missed payments' >> beam.Map(personalLoanDef)
        | 'Filter defaulters only' >> beam.Filter(filterDefaulters)
        | 'Output formatting 2' >> beam.Map(output_format)
        | 'Write personal' >> beam.io.WriteToText('./data/personal_loan_def.txt')
    )