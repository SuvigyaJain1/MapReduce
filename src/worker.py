from flask import Flask, request
import sys
import os
from requests import get as http_get

class Worker:
    def __init__(self, name, port, master):
        self.port = port
        self.app = Flask(name)
        self.master = master

        id = http_get(f"localhost:{master}/register/{self.port}").text
        assert id != -1 
        self.id = id 

    def start_server(self):

        @self.app.route("/")
        def probe():
            return "Alive"
        
        @self.app.route("/map")
        def start_map():
            if not self.partition or not self.task_name:
                return
            
            body = request.get_json()
            assert "partition" in body
            assert "mapper_path" in body

            mapper = body["mapper_path"]

            file = list(filter(lambda x: x.startswith("input-"), os.listdir(f"filesystem/{self.task_name}/{self.partition}")))[0]
            input_file = os.path.join("filesystem", str(self.task_name), str(self.partition), file)
            output_file = input_file.replace("input-", "int-", 1)
            os.system(f"python3 {mapper} {input_file} {output_file}")

        self.app.run("127.0.0.1", self.port)

if __name__ == "__main__":
    PORT = sys.argv[1] 
    MASTER = sys.argv[2] | 5000

    worker = Worker(__name__, PORT, MASTER)
    worker.start_server()