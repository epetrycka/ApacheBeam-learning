from typing import List, Tuple, Dict, Iterable
import apache_beam as beam

percent_of_amount_to_clear_monthly = 0.7

def split(record: str) -> List[str]:
    return record.split(',')

class ShowFraudPoints(beam.DoFn):
    def process(self, element: List[str]) -> List[Tuple[str, int]]:
        customer = element[0]
        fraud_points = 0

        max_credit_limit = float(element[5])
        total_spend = float(element[6])
        cleared_amount = float(element[8])

        if cleared_amount < (total_spend * percent_of_amount_to_clear_monthly):
            fraud_points += 1

        if total_spend == max_credit_limit and total_spend != cleared_amount:
            fraud_points += 1
        
        if fraud_points == 2:
            fraud_points += 1

        yield (customer, fraud_points)
    
def filterDefaulterCustomers(record: Tuple[str, int]) -> bool:
    return record[1] > 0

def showNames(record: List[str]) -> Tuple[str, str]:
    return (record[0], str(record[1]) + ' ' + str(record[2]))

def string(record: Tuple[str, Tuple[Iterable[int], Iterable[str]]]) -> str:
    fraud_points = ', '.join(map(str, record[1][0]))
    names = ', '.join(record[1][1])
    return f"{record[0]}, {fraud_points}, {names}"

with beam.Pipeline() as p:
    input = (
        p
        | 'Read informations about card users' >> beam.io.ReadFromText('./data/cards.txt', skip_header_lines=1)
        | 'Split by delimiter' >> beam.Map(split)
    )

    defaulter = (
        input
        | 'Check card skippers' >> beam.ParDo(ShowFraudPoints())
        | 'Group and sum fraud points' >> beam.CombinePerKey(sum)
        | 'Filter only defaulter customers' >> beam.Filter(filterDefaulterCustomers)
    )

    names = (
        input
        | 'Dictr (id, fullName)' >> beam.Map(showNames)
        | 'Distinct' >> beam.Distinct()
    )

    namesOfdefaulters = (
        {defaulter, names}
        | 'Join names' >> beam.CoGroupByKey()
        | 'String plis' >> beam.Map(string)
        | 'Write answers' >> beam.io.WriteToText('./data/defaulter_customers')
    )
