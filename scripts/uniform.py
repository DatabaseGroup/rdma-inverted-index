#!/usr/bin/python3

import sys
from numpy import random
from tqdm import tqdm


if __name__ == "__main__":
    if len(sys.argv) < 6:
        sys.exit("usage: ./uniform <num_records> <universe_size> <min_length> <avg_length> <max_length>")

    num_records = int(sys.argv[1])
    universe_size = int(sys.argv[2])
    min_length = int(sys.argv[3])
    avg_length = int(sys.argv[4])
    max_length = int(sys.argv[5])

    assert min_length >= 1
    assert max_length <= universe_size

    records = []
    rng = random.default_rng()

    for _ in tqdm(range(num_records)):
        record_length = min(max(min_length, random.poisson(avg_length)), max_length)
        record = rng.choice(universe_size, size=record_length, replace=False)
        record = sorted(record)
        records.append(record)

    for record in sorted(records, key=lambda x: (len(x), x)):
        print(" ".join([str(t + 1) for t in record]))
