import json
import logging
import os
import pathlib

from flask import Flask
from flask import request
import subprocess

app = Flask(__name__)


@app.route("/run_task_instance", methods=['POST'])
def run_task_instance():
    data = request.json

    # setup xcom input and output
    annotations = data["annotations"]
    base_path = pathlib.Path('/home/airflow') / annotations["dag_id"] / annotations["task_id"] / annotations["run_id"] / str(annotations["map_index"])
    os.makedirs(base_path, exist_ok=True)
    input_path = base_path / "input"
    with open(input_path, "w") as f:
        json.dump(data["xcoms"], f)
    airflow_output_path = f'{base_path}/output'
    try:
        os.remove(airflow_output_path)
    except FileNotFoundError:
        pass

    # execute task
    p = subprocess.Popen(data["args"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
    logging.info(f"exitcode: {p.returncode}; stdout: {p.stdout.read()}; stderr: {p.stderr.read()}")

    # retrieve xcom outputs
    try:
        with open(airflow_output_path, 'r') as f:
            xcoms = [json.loads(line) for line in f.readlines()]
    except FileNotFoundError:
        xcoms = []
    return json.dumps({"xcoms": xcoms})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='50000')
