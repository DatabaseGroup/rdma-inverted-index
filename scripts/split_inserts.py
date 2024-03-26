import sys

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(f"usage: ./{sys.argv[0]} <query_file>")

    query_file = sys.argv[1]

    with open(query_file) as f:
        for line in f:
            split_line = line.strip().split(" ")
            if split_line[0] == 'i:':
                doc_id = split_line[1]
                for x in split_line[2:]:
                    print(f"i: {doc_id} {x}")
            else:
                print(line.strip())
