import pendulum

from airflow.decorators import dag, task

from timing import timing


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)
def etl_example_dynamic():
    @task
    @timing
    def extract():
        # dummy data source
        return list({"1001": 301.27, "1002": 433.21, "1003": 502.22}.values())

    @task
    @timing
    def convert_currency(x):
        return 1.5 * x

    @task
    @timing
    def transform(order_values):
            total_order_value = 0
            for value in order_values:
                total_order_value += value

            return {"total_order_value": total_order_value}

    @task
    @timing
    def load(data):
        # dummy data sink
        print(data)

    # specify data flow
    converted = convert_currency.expand(x=extract())
    load(transform(converted))

# execute dag
etl_dag = etl_example_dynamic()
