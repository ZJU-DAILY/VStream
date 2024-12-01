# Description: This script calculates the parallelism for each node in the cluster and updates the flink-conf.yaml file
# on each node with the new parallelism.
# This script is expected to run on a cluster node.
import yaml
import sys
import math
import os

filename = sys.argv[1]
nodes = [
    "node10",
    "node11",
    "node12",
    "node13",
    "node14",
    "node15",
    "node182",
    "node21",
    "node22",
    "node23",
]
ssh_port = 4399


def get_parallelism(yaml_file):
    with open(yaml_file, "r") as stream:
        data = yaml.safe_load(stream)
        combiner_parallelism = data["parallelism"]
        reducer_parallelism = data["reduceParallelism"]
    return combiner_parallelism + reducer_parallelism + 7


if __name__ == "__main__":
    parallelism = math.ceil(get_parallelism(filename) / len(nodes))
    print(
        "calculate-parallelism.py: new parallelism is {}".format(parallelism),
        file=sys.stderr,
    )
    for node in nodes:
        # change taskmanager.numberOfTaskSlots to parallelism
        command = r"""ssh -p {} {} 'sed -i "s/^\(taskmanager\.numberOfTaskSlots:\s*\).*/\1{}/" "$FLINK_HOME/conf/flink-conf.yaml"'""".format(
            ssh_port, node, parallelism
        )
        os.system(command)
