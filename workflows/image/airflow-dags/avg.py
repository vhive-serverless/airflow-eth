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
def compute_avg():
    @task
    @timing
    def extract(params=None):
        # dummy data source
        logging.info(f"params: {params}")
        return params["data"]

    @task
    @timing
    def convert_types(numbers_list):
        traceback.print_stack(file=sys.stderr)
        return list(map(float, numbers_list))

    @task
    @timing
    def do_avg(data):
        # dummy data sink
        return sum(data)/len(data)

    # specify data flow
    do_avg(convert_types(extract()))

# execute dag
avg_dag = compute_avg()
