from flask import Flask
from flask import request
import subprocess

app = Flask(__name__)

@app.route("/run_task_instance", methods=['POST'])
def run_task_instance():
    data = request.json
    p = subprocess.Popen(data["args"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p.wait()
    return f"exitcode: {p.returncode}; stdout: {p.stdout.read()}; stderr: {p.stderr.read()}"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port='50000')
