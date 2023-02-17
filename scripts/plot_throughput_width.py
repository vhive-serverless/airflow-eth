import collections
import pathlib
import re

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


# extract throughput information from gateway server logs
def get_throughput(benchmark_logs_path, filter_func):
    data_by_width = collections.defaultdict(lambda: [])

    for gateway_log_path in benchmark_logs_path.glob("log_gateway*"):
        with open(gateway_log_path) as f:
            lines = f.readlines()
        for line in lines:
            m = re.match(r"Running (?P<workflow>\S+) took ((?P<minutes>[0-9]+)m)?(?P<seconds>[0-9.]+)s", line)
            if m:
                workflow = m.group("workflow")
                if not filter_func(workflow):
                    continue
                time_seconds = float(m.group("seconds"))
                time_minutes = m.group("minutes")
                if time_minutes is not None:
                    time_seconds += int(time_minutes) * 60
                width = int(re.match(r".*_w([0-9]+)_", workflow).group(1))
                throughput = (2 + width) / time_seconds
                data_by_width[width].append(throughput)
    return data_by_width

benchmark_knative_logs_path = pathlib.Path("./benchmark_data/benchmarking_logs_1676206749")
data_knative = get_throughput(benchmark_knative_logs_path, lambda x: "_d3" in x)
benchmark_stock_logs_path = pathlib.Path("./benchmark_data/benchmarking_logs_stock_1676240305")
data_stock = get_throughput(benchmark_stock_logs_path, lambda x: "_d3" in x)

print(f"Found data for {len(data_knative)} different workflows.")

# Ian Hincks, https://stackoverflow.com/questions/33864578/matplotlib-making-labels-for-violin-plots, 2023-02-13
labels = []
def add_label(violin, label):
    color = violin["bodies"][0].get_facecolor().flatten()
    labels.append((mpatches.Patch(color=color), label))


def do_violinplot_with_label(data, label):
    x_values = []
    y_values = []
    count = 0
    sum_througput = 0
    for width, values in data.items():
        x_values.append(width)
        y_values.append(values)
        sum_througput += sum(values)
        count += len(values)
    print(f"Average throughput for {label}: {sum_througput/count:.2f} tasks / s")
    p = plt.violinplot(y_values, x_values, widths=1.5)
    add_label(p, label)


do_violinplot_with_label(data_knative, "Airflow + Knative")
do_violinplot_with_label(data_stock, "Airflow w/o Knative")

plt.xlabel("Workflow Depth [number of functions]")
plt.ylabel("Latency [s]")
plt.title("End-to-End latency comparison between Airflow+Knative\nand stock Airflow for various workflow depths")
plt.legend(*zip(*labels), loc="upper left", bbox_to_anchor=(0, -0.15), ncol=2)
output_path = "throughput_width.pdf"
print(f"Saving plot to {output_path}")
plt.savefig(output_path, bbox_inches='tight')
