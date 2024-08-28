import sys
from tqdm import tqdm


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(f"usage: ./{sys.argv[0]} <input-file> > <output-file>")

    input_file = sys.argv[1]

    max_len = 0
    list_key = 0
    max_id = 0

    with open(input_file) as f:
        num_lines = sum(1 for _ in f)

    with open(input_file) as f:
        current_list = (list_key, [])
        prev_key = -1

        for line in tqdm(f, total=num_lines):
            x, y = line.strip().split(" ")[:2]
            if x != '%':
                if x == prev_key:
                    current_list[1].append(y)
                else:
                    if prev_key != -1:
                        print(f"{current_list[0]}: {' '.join(current_list[1])}")

                    max_len = max(max_len, len(current_list[1]))
                    current_list = (list_key, [y])
                    list_key += 1

                max_id = max(max_id, int(y))
                prev_key = x

        # output final set
        print(f"{current_list[0]}: {' '.join(current_list[1])}")

    print(f"\nlargest list: {max_len}", file=sys.stderr)
    print(f"universe size: {list_key}", file=sys.stderr)
    print(f"max id: {max_id}", file=sys.stderr)
