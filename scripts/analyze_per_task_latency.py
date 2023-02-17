import collections
import json
import re
import sys

import pandas as pd


data = collections.defaultdict(lambda: [])
data_compact = collections.defaultdict(lambda: {})

pd.options.display.float_format = '{:f}'.format

if len(sys.argv) < 2:
    print("Usage: analyze_per_task_latency.py <scheduler_log>")
    exit(1)
with open(sys.argv[1]) as f:
    scheduler_log_data = f.read()

ts_types = set()
for line in scheduler_log_data.split('\n'):
    m = re.match(r".*TIMING: (.*)", line)
    if m:
        line_data = json.loads(m.group(1))
        ts_types.update((f"{line_data['function']}_{x}" for x in line_data["timestamp_annotations"]))
for line in scheduler_log_data.split('\n'):
    m = re.match(r".*TIMING: (.*)", line)
    if m:
        line_data = json.loads(m.group(1))
        log_key = (line_data["dag_id"], line_data["task_id"], line_data["run_id"], line_data["map_index"], line_data["try_number"])

        for key in set(line_data.keys()) - {"times", "timestamp_annotations"}:
            data[key].append(line_data[key])
        line_ts_annotations = tuple(f'{line_data["function"]}_{x}' for x in line_data["timestamp_annotations"])
        for ts, ts_type in zip(line_data["times"], line_ts_annotations):
            data[ts_type].append(ts)
            data_compact[log_key][ts_type] = ts
        for ts_type in ts_types.difference(line_ts_annotations):
            data[ts_type].append(None)

reshaped_compact_data = collections.defaultdict(list)
for log_key, timestamps in data_compact.items():
    dag_id, task_id, run_id, map_index, try_number = log_key
    reshaped_compact_data["dag_id"].append(dag_id)
    reshaped_compact_data["task_id"].append(task_id)
    reshaped_compact_data["run_id"].append(run_id)
    reshaped_compact_data["map_index"].append(map_index)
    reshaped_compact_data["try_number"].append(try_number)
    for ts_type, ts in timestamps.items():
        reshaped_compact_data[ts_type].append(ts)

# df = pd.DataFrame(data)
df_compact = pd.DataFrame(reshaped_compact_data)
df_w1 = df_compact[df_compact['dag_id'].str.contains("_w1_")]
df_send_post = df_w1["executor_async_task_before_post_request"] - df_w1["executor_run_pod_async_function_entry"]
df_receive_post = df_w1["flask_run_task_instance_function_entry"] - df_w1["executor_run_pod_async_function_entry"]
df_function_started = df_w1["flask_run_task_instance_after_start_subprocess"] - df_w1["executor_run_pod_async_function_entry"]
df_function_finished = df_w1["flask_run_task_instance_after_finished_subprocess"] - df_w1["executor_run_pod_async_function_entry"]
df_task_finished = df_w1["executor_async_task_function_exit"] - df_w1["executor_run_pod_async_function_entry"]

stats = []
pretty_names = ["POST request sent","POST request received","Subprocess started","Subprocess done","Total"]
dataframes = [df_send_post, df_receive_post, df_function_started, df_function_finished, df_task_finished]
print("All reported as min, mean, median, max")
for name, df in zip(pretty_names, dataframes):
    stats.append([df.min(), df.median(), df.mean(), df.max()])
    print(f"{name}: {df.min():.2f}, {df.median():.2f}, {df.mean():.2f}, {df.max():.2f}")

print("For latex:")
for name, stats_line in zip(pretty_names, stats):
    print(name + " & " + " & ".join(map(lambda x: format(x, ".2f"), stats_line)) + r" \\")
