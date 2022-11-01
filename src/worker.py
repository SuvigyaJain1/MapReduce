from flask import Flask, request
import sys
import os
from requests import get as http_get
from utils.common import get_current_time

class Worker:
    def __init__(self, name, port, master):
        self.port = port
        self.app = Flask(name)
        self.master = master
        self.partitions = []
        self.task_name = ""

        id = http_get(f"http://127.0.0.1:{master}/register/{self.port}").text
        print(f"{get_current_time()} Registered Worker with master")
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
            assert "mapper_path" in body
            mapper = body["mapper_path"]

            for partition in self.partitions:
                file = list(filter(lambda x: x.startswith("input-"), os.listdir(f"filesystem/{self.task_name}/{partition}")))[0]
                input_file = os.path.join("filesystem", str(self.task_name), str(partition), file)
                output_file = input_file.replace("input-", "int-", 1)
                os.system(f"python3 {mapper} {input_file} {output_file}")

        @self.app.route("/reduce")
        def start_red():
            if not self.partition or not self.task_name:
                return 
            
            body = request.get_json()
            assert "reducer_path" in body
            reducer = body["reducer_path"]

            for partition in self.partitions:
                file = list(filter(lambda x: x.startswith("redinput-"), os.listdir(f"filesystem/{self.task_name}/{partition}")))[0]
                input_file = os.path.join("filesystem", str(self.task_name), str(partition), file)
                output_file = input_file.replace("redinput-", "out-", 1)
                os.system(f"python3 {reducer} {input_file} {output_file}")

        @self.app.route("/assign-partition/<partition_id>")
        def register(partition_id):
            self.partitions.append(partition_id)

        @self.app.route("/assign-task/<task_name>")
        def assign_task(task_name):
            if self.task_name != task_name:
                self.partitions = []
                self.task_name = task_name

        self.app.run("127.0.0.1", self.port)

if __name__ == "__main__":
    PORT = sys.argv[1] 
    MASTER = sys.argv[2]

    worker = Worker(__name__, PORT, MASTER)
    worker.start_server()