durations = []
accuracies = []
sst_counts = []

log_file = "/home/auroflow/code/vector-search/rocksdb-stream/tmp/rocksdb-standalone-2023-12-21T21-26-10/LOG"
console_file = "/home/auroflow/code/vector-search/rocksdb-stream/tmp/rocksdb-standalone-2023-12-21T21-26-10/console.txt"

with open(console_file) as f:
    for line in f:
        if line.startswith("Query"):
            duration = int(line.split(" ")[4])
            durations.append(duration)
        elif "queries: " in line:
            # parse the array
            array_str = line.split(" ", 2)[-1]
            array_str = array_str.replace(" ", "")
            array_str = array_str.replace("]", "")
            array_str = array_str.replace("[", "")
            array = array_str.split(",")
            array = [float(x) for x in array]
            accuracies = array

with open(log_file) as f:
    for line in f:
        if "SSTables" in line:
            sst_counts.append(int(line.split(" ")[-2]))

for i, (duration, accuracy, sst_count) in enumerate(zip(durations, accuracies, sst_counts)):
    print(i, duration, accuracy, sst_count)

