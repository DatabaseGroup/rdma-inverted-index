#!/usr/bin/python3

import sys
from tqdm import tqdm


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(f"usage: ./{sys.argv[0]} <input-file> <universe-size> > <output-file>")

    input_file = sys.argv[1]
    universe = int(sys.argv[2])
    index = [[] for _ in range(universe + 1)]

    with open(input_file) as f:
        num_lines = sum(1 for _ in f)

    doc_id = 0
    with open(input_file) as f:
        for line in tqdm(f, total=num_lines):
            for token in line.split(" "):
                index[int(token)].append(str(doc_id))
            doc_id += 1

    num_lists = sum([1 for inv_list in index if inv_list])

    print(universe)
    print(num_lists)
    for token, inv_list in enumerate(index):
        if inv_list:
            print(f"{token:} {' '.join(inv_list)}")
