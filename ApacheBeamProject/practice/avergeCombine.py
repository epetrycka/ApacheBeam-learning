import apache_beam as beam

class CalculateAverage(beam.CombineFn):
    def create_accumulator(self) -> tuple[float, int]:
        return (0.0, 0)
    
    def add_input(self, accumulator: tuple[float, int], element: float) -> tuple[float, int]:
        (nums, count) = accumulator
        return (nums + element, count+1)
        
    def merge_accumulators(self, accumulators: list[tuple[float, int]]) -> tuple[float, int]:
        nums, count = zip(*accumulators)
        return (sum(nums), sum(count))
    
    def extract_output(self, accumulator: tuple[float, int]) -> float:
        (nums, count) = accumulator
        return nums/count if count!=0 else float('nan')

with beam.Pipeline() as p:
    calculate_average = (
        p
        | beam.Create([10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])
        | beam.CombineGlobally(CalculateAverage())
        | beam.io.WriteToText('./data/average.txt')
    )