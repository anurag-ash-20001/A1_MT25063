#!/usr/bin/env python
"""
Task 1 / Task 4 - MapReduce (mrjob) aggregation.

For every non-null PULocationID, compute:
    (i)  trip_count  - number of trips
    (ii) avg_fare    - average fare_amount
considering only rows where fare_amount is non-null and fare_amount >= 0.

Mapper key:   PULocationID (string)
Mapper value: (1, fare_amount)   -- one trip and its fare
Shuffle boundary: for a given PULocationID, every (1, fare_amount) pair
    emitted by any mapper for that key is grouped together and sent to
    the reducer responsible for that key. That is the only information
    that crosses the shuffle boundary - individual trip rows never
    leave the mapper, only the per-trip (count, fare) pair does.

Run with the local runner (simulates multiple mappers/reducers via
subprocesses, closer to real Hadoop semantics than -r inline):

    python mapreduce/task1.py -r local taxi_1m.csv > mr_task1_output.txt

Or drive it programmatically for timing (see utils/run_experiments.py).
"""
import csv
import io
import math

from mrjob.job import MRJob
from mrjob.step import MRStep

PU_LOCATION_ID_COL = "PULocationID"
FARE_AMOUNT_COL = "fare_amount"

# Fallback column positions for the standard 2026 TLC Yellow Taxi schema,
# used only if a split somehow doesn't see the header line first.
FALLBACK_PU_IDX = 7
FALLBACK_FARE_IDX = 10


class MRTaxiAggregation(MRJob):

    def mapper_init(self):
        self.header = None
        self.pu_idx = None
        self.fare_idx = None

    def mapper(self, _, line):
        if not line.strip():
            return
        try:
            row = next(csv.reader(io.StringIO(line)))
        except StopIteration:
            return

        if self.header is None:
            if PU_LOCATION_ID_COL in row:
                self.header = row
                self.pu_idx = row.index(PU_LOCATION_ID_COL)
                self.fare_idx = row.index(FARE_AMOUNT_COL)
                return  # this line was the header, not data
            else:
                self.header = []
                self.pu_idx = FALLBACK_PU_IDX
                self.fare_idx = FALLBACK_FARE_IDX

        if len(row) <= max(self.pu_idx, self.fare_idx):
            return

        pu = row[self.pu_idx].strip()
        fare_raw = row[self.fare_idx].strip()

        if not pu or pu.lower() == "none":
            return
        try:
            fare = float(fare_raw)
        except (ValueError, TypeError):
            return
        if not math.isfinite(fare) or fare < 0:
            return

        yield pu, (1, fare)

    def combiner(self, pu, counts_fares):
        total_count = 0
        total_fare = 0.0
        for count, fare in counts_fares:
            total_count += count
            total_fare += fare
        yield pu, (total_count, total_fare)

    def reducer(self, pu, counts_fares):
        total_count = 0
        total_fare = 0.0
        for count, fare in counts_fares:
            total_count += count
            total_fare += fare
        avg_fare = total_fare / total_count if total_count else 0.0
        yield pu, {
            "PULocationID": pu,
            "trip_count": total_count,
            "avg_fare": avg_fare,
        }

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer,
            )
        ]


if __name__ == "__main__":
    MRTaxiAggregation.run()
