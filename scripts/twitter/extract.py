import sys
from tqdm import tqdm


if __name__ == "__main__":
    max_len = 0
    list_key = 0
    max_id = 0

    with open("out.twitter_mpi") as f:
        current_list = (list_key, [])
        prev_key = -1

        for line in tqdm(f.readlines()):
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
