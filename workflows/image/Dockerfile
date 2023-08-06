FROM apache/airflow:2.4.3
STOPSIGNAL SIGINT
RUN pip install grpcio grpcio-tools
COPY airflow-dags /opt/airflow/dags
COPY airflow /home/airflow/.local/lib/python3.7/site-packages/airflow
# COPY flask_worker_entrypoint.py /opt/airflow/flask_worker_entrypoint.py
# ENTRYPOINT ["flask", "--app", "/opt/airflow/flask_worker_entrypoint.py", "run", "--host", "0.0.0.0", "--port", "50000"]
EXPOSE 8081
ENTRYPOINT ["python", "/home/airflow/.local/lib/python3.7/site-packages/airflow/grpc/remote_xcom/grpc_worker_entrypoint.py"]
