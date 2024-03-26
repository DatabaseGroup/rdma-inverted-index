import sys
import random

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(f"usage: ./{sys.argv[0]} <num_queries>")

    num_queries = int(sys.argv[1])

    q1 = [0, 1, 2]
    q2 = [3, 4, 5]
    q3 = [6, 7, 8, 9]
    q4 = [10, 11]
    q5 = [12, 13, 14]
    q6 = [15, 16]
    q7 = [17, 18, 19, 20]
    q8 = [21, 22, 23, 24]
    q9 = [25, 26, 27, 28]
    queries = [q1, q2, q3, q4, q5, q6, q7, q8, q9]

    for _ in range(num_queries):
        print(f"r: {' '.join([str(key) for key in random.choice(queries)])}")
