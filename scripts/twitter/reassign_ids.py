import sys
from tqdm import tqdm


if __name__ == "__main__":
    dictionary = {}
    next_token = 0

    with open("twitter-lists.txt") as f:
        for line in tqdm(f.readlines()):
            key, tokens = line.strip().split(": ")
            new_tokens = []

            for token in tokens.strip().split(" "):
                if token not in dictionary:
                    dictionary[token] = next_token
                    next_token += 1

                new_tokens.append(str(dictionary[token]))

            print(f"{key}: {' '.join(new_tokens)}")

    print(f"max token id: {next_token - 1}", file=sys.stderr)
