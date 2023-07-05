import pendulum
from airflow.decorators import dag, task
import logging
from functools import wraps
from time import time


# by Jonathan Prieto-Cubides https://stackoverflow.com/questions/1622943/timeit-versus-timing-decorator
def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        logging.info('func:%r args:[%r, %r] took: %f sec. Start: %f, End: %f' % (f.__name__, args, kw, te-ts, ts, te))
        return result
    return wrap

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False)
def benchmark_w1_d2():
    @task
    @timing
    def extract(params=None):
        # dummy data source
        return params["data"]

    @task
    @timing
    def do_sum(values):
        return (values)

    # specify data flow
    intermediate_00 = extract()
    do_sum(intermediate_00)

# execute dag
etl_dag = benchmark_w1_d2()
