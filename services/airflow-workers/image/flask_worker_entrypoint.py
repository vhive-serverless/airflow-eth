import json
import logging
import os

from flask import Flask
from flask import request
import subprocess

app = Flask(__name__)

@app.route("/run_task_instance", methods=['POST'])
def run_task_instance():
    airflow_input_path = '/home/airflow/input'
    airflow_output_path = '/home/airflow/output'
    downstream_task_ids_path = '/home/airflow/downstream_task_ids'

    data = request.json
    with open(airflow_input_path, 'w') as f:
        json.dump(data["xcoms"], f)

    try:
        os.remove(airflow_output_path)
    except FileNotFoundError:
        pass

    p = subprocess.Popen(data["args"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
    logging.info(f"exitcode: {p.returncode}; stdout: {p.stdout.read()}; stderr: {p.stderr.read()}")
    with open(airflow_output_path, 'rb') as f:
        xcoms = [json.loads(line) for line in f.readlines()]
    with open(downstream_task_ids_path) as f:
        downstream_task_ids = json.load(f)
    return json.dumps({"xcoms": xcoms, "downstream_task_ids": downstream_task_ids})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='50000')
