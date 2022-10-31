import requests

body = {}
response = requests.post("127.0.0.1:5000", json=body)
print(response.status_code, response.text, response.content, sep="\n")