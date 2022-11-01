#!/usr/bin/python3
import requests
import sys

try:
    taskname=sys.argv[1]
    mapper=sys.argv[2]
    reducer=sys.argv[3]
    inp=sys.argv[4]
except:
    print("Only "+str(len(sys.argv))+" arguments are given")
    print("Command should be in the form of ./mapred.py <task_name> <path to mapper> <path to reducer> <path to input>")
    sys.exit()
body = {
    "task_name": taskname,
    "mapper_file":mapper,
    "reducer_file":reducer,
    "input_file":inp
}
print(body)
response = requests.post("http://127.0.0.1:5000/schedule", json=body)
if(response.status_code==200):
    print("Task was succesfully completed!")
    print(response.text, sep="\n")
else:
    print("Task failed to complete")
    print(response.status_code,response.text,response.content,sep='\n')