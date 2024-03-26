import sys
import random
import numpy as np


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(f"usage: ./{sys.argv[0]} <popular-terms-file> <num-queries>")

    min_length = 2
    avg_length = 5
    max_length = 10

    popular_terms_file = str(sys.argv[1])
    num_queries = int(sys.argv[2])

    popular_terms = []
    queries = []

    with open(popular_terms_file) as f:
        for line in f.readlines():
            term = line.split(":")[0].strip()
            popular_terms.append(term)

    for _ in range(num_queries):
        record_length = min(max(min_length, np.random.poisson(avg_length)), max_length)
        query = random.sample(popular_terms, record_length)
        print(f"r: {' '.join([str(term) for term in sorted(query)])}")
