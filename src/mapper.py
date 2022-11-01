import sys
import re

input_file = sys.argv[1]
output_file = sys.argv[2]

text = ""
with open(input_file, "r") as f:
    text = f.read().strip().split()
    for i in range(len(text)):
        text[i] = re.sub(r'\W+', '', text[i]) #remove punctuation

output = "\n".join(list(map(lambda x: f"{x.lower()}, {1}", text)))
with open(output_file, "w") as f:
    print(output, file=f)