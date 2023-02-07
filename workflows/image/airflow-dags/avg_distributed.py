import logging
import sys
import traceback

import pendulum

from airflow.decorators import dag, task

from timing import timing

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)
def compute_avg_distributed():
    @task
    @timing
    def extract(params=None):
        # dummy data source
        logging.info(f"params: {params}")
        return params["data"]

    @task
    @timing
    def compute_sum(numbers_list):
        traceback.print_stack(file=sys.stderr)
        return sum(map(float, numbers_list))

    @task
    @timing
    def compute_count(numbers_list):
        traceback.print_stack(file=sys.stderr)
        return len(numbers_list)

    @task
    @timing
    def do_avg(total, count):
        # dummy data sink
        if count != 0:
            return total / count
        else:
            return 0

    # specify data flow
    e = extract()
    s = compute_sum(e)
    c = compute_count(e)
    do_avg(s, c)

# execute dag
avg_dag = compute_avg_distributed()
