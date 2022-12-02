import pendulum

from airflow.decorators import dag, task

from time import sleep

from timing import timing

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)
def sleeper_agent():
    @task
    @timing
    def learn():
        sleep(5)
        return {"skills": ["stealth", "disguise"]}

    @task
    @timing
    def select_action(attrs):
        sleep(2)
        return {"action": attrs["skills"][1]}

    @task
    @timing
    def activate(x):
        sleep(3)
        print(x["action"])

    # specify data flow
    activate(select_action(learn()))

# execute dag
etl_dag = sleeper_agent()

