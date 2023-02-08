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
def benchmark_w24_d4():
    @task
    @timing
    def extract():
        # dummy data source
        return 10

    @task
    @timing
    def stage_00(x: int):
        return x + x

    @task
    @timing
    def stage_01(x: int):
        return x + x

    @task
    @timing
    def stage_02(x: int):
        return x + x

    @task
    @timing
    def stage_03(x: int):
        return x + x

    @task
    @timing
    def stage_04(x: int):
        return x + x

    @task
    @timing
    def stage_05(x: int):
        return x + x

    @task
    @timing
    def stage_06(x: int):
        return x + x

    @task
    @timing
    def stage_07(x: int):
        return x + x

    @task
    @timing
    def stage_08(x: int):
        return x + x

    @task
    @timing
    def stage_09(x: int):
        return x + x

    @task
    @timing
    def stage_10(x: int):
        return x + x

    @task
    @timing
    def stage_11(x: int):
        return x + x

    @task
    @timing
    def stage_12(x: int):
        return x + x

    @task
    @timing
    def stage_13(x: int):
        return x + x

    @task
    @timing
    def stage_14(x: int):
        return x + x

    @task
    @timing
    def stage_15(x: int):
        return x + x

    @task
    @timing
    def stage_16(x: int):
        return x + x

    @task
    @timing
    def stage_17(x: int):
        return x + x

    @task
    @timing
    def stage_18(x: int):
        return x + x

    @task
    @timing
    def stage_19(x: int):
        return x + x

    @task
    @timing
    def stage_20(x: int):
        return x + x

    @task
    @timing
    def stage_21(x: int):
        return x + x

    @task
    @timing
    def stage_22(x: int):
        return x + x

    @task
    @timing
    def stage_23(x: int):
        return x + x

    @task
    @timing
    def stage_24(x: int):
        return x+x

    @task
    @timing
    def stage_25(x: int):
        return x+x

    @task
    @timing
    def stage_26(x: int):
        return x+x

    @task
    @timing
    def stage_27(x: int):
        return x+x

    @task
    @timing
    def stage_28(x: int):
        return x+x

    @task
    @timing
    def stage_29(x: int):
        return x+x

    @task
    @timing
    def stage_30(x: int):
        return x+x

    @task
    @timing
    def stage_31(x: int):
        return x+x

    @task
    @timing
    def stage_32(x: int):
        return x+x

    @task
    @timing
    def stage_33(x: int):
        return x+x

    @task
    @timing
    def stage_34(x: int):
        return x+x

    @task
    @timing
    def stage_35(x: int):
        return x+x

    @task
    @timing
    def stage_36(x: int):
        return x+x

    @task
    @timing
    def stage_37(x: int):
        return x+x

    @task
    @timing
    def stage_38(x: int):
        return x+x

    @task
    @timing
    def stage_39(x: int):
        return x+x

    @task
    @timing
    def stage_40(x: int):
        return x+x

    @task
    @timing
    def stage_41(x: int):
        return x+x

    @task
    @timing
    def stage_42(x: int):
        return x+x

    @task
    @timing
    def stage_43(x: int):
        return x+x

    @task
    @timing
    def stage_44(x: int):
        return x+x

    @task
    @timing
    def stage_45(x: int):
        return x+x

    @task
    @timing
    def stage_46(x: int):
        return x+x

    @task
    @timing
    def stage_47(x: int):
        return x+x

    @task
    @timing
    def do_sum(values):
        return sum(values)

    # specify data flow
    intermediate_00 = extract()
    intermediate_01 = stage_00(x=intermediate_00)
    intermediate_02 = stage_01(x=intermediate_00)
    intermediate_03 = stage_02(x=intermediate_00)
    intermediate_04 = stage_03(x=intermediate_00)
    intermediate_05 = stage_04(x=intermediate_00)
    intermediate_06 = stage_05(x=intermediate_00)
    intermediate_07 = stage_06(x=intermediate_00)
    intermediate_08 = stage_07(x=intermediate_00)
    intermediate_09 = stage_08(x=intermediate_00)
    intermediate_10 = stage_09(x=intermediate_00)
    intermediate_11 = stage_10(x=intermediate_00)
    intermediate_12 = stage_11(x=intermediate_00)
    intermediate_13 = stage_12(x=intermediate_00)
    intermediate_14 = stage_13(x=intermediate_00)
    intermediate_15 = stage_14(x=intermediate_00)
    intermediate_16 = stage_15(x=intermediate_00)
    intermediate_17 = stage_16(x=intermediate_00)
    intermediate_18 = stage_17(x=intermediate_00)
    intermediate_19 = stage_18(x=intermediate_00)
    intermediate_20 = stage_19(x=intermediate_00)
    intermediate_21 = stage_20(x=intermediate_00)
    intermediate_22 = stage_21(x=intermediate_00)
    intermediate_23 = stage_22(x=intermediate_00)
    intermediate_24 = stage_23(x=intermediate_00)
    intermediate_25 = stage_24(x=intermediate_01)
    intermediate_26 = stage_25(x=intermediate_02)
    intermediate_27 = stage_26(x=intermediate_03)
    intermediate_28 = stage_27(x=intermediate_04)
    intermediate_29 = stage_28(x=intermediate_05)
    intermediate_30 = stage_29(x=intermediate_06)
    intermediate_31 = stage_30(x=intermediate_07)
    intermediate_32 = stage_31(x=intermediate_08)
    intermediate_33 = stage_32(x=intermediate_09)
    intermediate_34 = stage_33(x=intermediate_10)
    intermediate_35 = stage_34(x=intermediate_11)
    intermediate_36 = stage_35(x=intermediate_12)
    intermediate_37 = stage_36(x=intermediate_13)
    intermediate_38 = stage_37(x=intermediate_14)
    intermediate_39 = stage_38(x=intermediate_15)
    intermediate_40 = stage_39(x=intermediate_16)
    intermediate_41 = stage_40(x=intermediate_17)
    intermediate_42 = stage_41(x=intermediate_18)
    intermediate_43 = stage_42(x=intermediate_19)
    intermediate_44 = stage_43(x=intermediate_20)
    intermediate_45 = stage_44(x=intermediate_21)
    intermediate_46 = stage_45(x=intermediate_22)
    intermediate_47 = stage_46(x=intermediate_23)
    intermediate_48 = stage_47(x=intermediate_24)
    do_sum((intermediate_25,
            intermediate_26,
            intermediate_27,
            intermediate_28,
            intermediate_29,
            intermediate_30,
            intermediate_31,
            intermediate_32,
            intermediate_33,
            intermediate_34,
            intermediate_35,
            intermediate_36,
            intermediate_37,
            intermediate_38,
            intermediate_39,
            intermediate_40,
            intermediate_41,
            intermediate_42,
            intermediate_43,
            intermediate_44,
            intermediate_45,
            intermediate_46,
            intermediate_47,
            intermediate_48))

# execute dag
etl_dag = benchmark_w24_d4()
