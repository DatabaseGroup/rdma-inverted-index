import sys
from tqdm import tqdm


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(f"usage: ./{sys.argv[0]} <input-file> > <output-file>")

    input_file = sys.argv[1]

    dictionary = {}
    next_token = 0

    with open(input_file) as f:
        num_lines = sum(1 for _ in f)

    with open(input_file) as f:
        for line in tqdm(f, total=num_lines):
            key, tokens = line.strip().split(": ")
            new_tokens = []

            for token in tokens.strip().split(" "):
                if token not in dictionary:
                    dictionary[token] = next_token
                    next_token += 1

                new_tokens.append(str(dictionary[token]))

            print(f"{key}: {' '.join(new_tokens)}")

    print(f"max token id: {next_token - 1}", file=sys.stderr)
