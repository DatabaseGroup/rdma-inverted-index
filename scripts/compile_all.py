#!/usr/bin/python3

from subprocess import Popen


if __name__ == "__main__":
    nodes = ["cluster1", "cluster2", "cluster3", "cluster4", "cluster5", "cluster6", "cluster7", "cluster8", "cluster9"]
    processes = []

    for node in nodes:
        command = f"./remote.sh {node} cleanbuild"
        print(command)
        process = Popen(command, shell=True)
        processes.append((node, process))

    for (node, process) in processes:
        process.wait()
        print(f"{node} done")
