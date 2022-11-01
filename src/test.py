import requests

body = {
    "task_name": "sample_task",
    "mapper_file":"mapper.py",
    "reducer_file":"reducer.py",
    "input_file":"sample.txt"
}

response = requests.post("http://127.0.0.1:5000/schedule", json=body)
print(response.status_code, response.text, response.content, sep="\n")