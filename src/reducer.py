import sys

input_file = sys.argv[1]
output_file = sys.argv[2]

count = {}
with open(input_file, "r") as fin, open(output_file, "w") as fout:
    for line in fin.readlines():
        key, val = line.split(',')
        count[key] = count.get(key, 0) + 1
    
    for key in count:
        print(f"{key}, {count[key]}", file=fout)
