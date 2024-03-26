import sys
from tqdm import tqdm


def read_int(f) -> int:
    endianness = "little"
    return int.from_bytes(f.read(4), endianness)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(f"usage: ./{sys.argv[0]} <binary-index-file> <k>")

    # The binary file looks as follows:
    # <universe-size><numer-of-lists><list-id><list-size><list-entry-1>...
    binary_index = str(sys.argv[1])
    k = int(sys.argv[2])

    popular_terms = []

    with open(binary_index, "br") as f:
        universe_size = read_int(f)
        num_lists = read_int(f)

        for _ in tqdm(range(num_lists)):
            term = read_int(f)
            list_size = read_int(f)

            if len(popular_terms) < k:
                popular_terms.append((term, list_size))
                popular_terms.sort(key=lambda x: x[1], reverse=True)
            elif list_size > popular_terms[-1][1]:
                popular_terms[-1] = (term, list_size)
                popular_terms.sort(key=lambda x: x[1], reverse=True)

            # go to next list
            f.read(4 * list_size)

        for term, frequency in popular_terms:
            print(f"{term}: {frequency}")
