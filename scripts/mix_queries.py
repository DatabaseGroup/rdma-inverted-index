
import sys
import random
import numpy as np
from collections import deque


if __name__ == "__main__":
    if len(sys.argv) < 5:
        sys.exit(f"usage: ./{sys.argv[0]} <read-query-file> <documents-to-insert-file> <output-file> <insert-percentage> <num-queries>")

    read_query_file = str(sys.argv[1])
    docs_to_insert_file = str(sys.argv[2])
    output_file = str(sys.argv[3])
    insert_percentage = int(sys.argv[4]) / 100
    num_queries = int(sys.argv[5])

    probabilities = [1 - insert_percentage, insert_percentage]
    modes = ["r", "i"]

    with open(read_query_file) as rq:
        read_lines = rq.readlines()
        random.shuffle(read_lines)
    with open(docs_to_insert_file) as wq:
        insert_lines = wq.readlines()
        random.shuffle(insert_lines)

    read_lines = deque(read_lines)
    insert_lines = deque(insert_lines)

    with open(output_file, "w") as f:
        for _ in range(num_queries):
            mode = np.random.choice(modes, size=1, p=probabilities)

            if (mode == "i"):
                line = insert_lines.popleft()
                doc = line.split()
                doc[0] = doc[0][:-1]  # get rid of the colon
                f.write(f"i: {' '.join(doc)}\n")
            else:
                line = read_lines.popleft()
                f.write(line)
