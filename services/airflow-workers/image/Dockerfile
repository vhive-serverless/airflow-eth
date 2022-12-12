FROM apache/airflow:2.4.3
COPY airflow-dags /opt/airflow/dags
COPY flask_poc.py /opt/airflow/flask_poc.py
ENTRYPOINT ["flask", "--app", "/opt/airflow/flask_poc.py", "run", "--host", "0.0.0.0", "--port", "50000"]
EXPOSE 50000
