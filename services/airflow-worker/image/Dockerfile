#FROM vhiveease/py_grpc:builder_grpc as builder_workload
#COPY --from=vhiveease/py_grpc:builder_grpc /root/.local /root/.local
#COPY requirements.txt .
#RUN pip3 install --user -r requirements.txt
#
#FROM apache/airflow:2.4.1 as var_workload
#COPY *.py /
#COPY --from=builder_workload /root/.local /root/.local
#RUN apk add libstdc++ --update --no-cache
#
#EXPOSE 50000
#
#STOPSIGNAL SIGKILL
#
#CMD ["/usr/local/bin/python", "/server.py"]
FROM apache/airflow:2.4.3
#COPY airflow.cfg /opt/airflow/airflow.cfg
#COPY airflow_local_settings.py /opt/airflow/config/airflow_local_settings.py
COPY airflow-dags /opt/airflow/dags
COPY flask_poc.py /opt/airflow/flask_poc.py
#ENTRYPOINT /bin/sh -c
#CMD ["flask", "--app", "/opt/airflow/flask_poc.py", "run", "--host", "0.0.0.0", "--port", "50000"]
ENTRYPOINT ["flask", "--app", "/opt/airflow/flask_poc.py", "run", "--host", "0.0.0.0", "--port", "50000"]
EXPOSE 50000
#CMD ["airflow", "tasks", "run", "etl_example_dynamic", "extract", "manual__2022-11-21T09:05:42.528202+00:00", "--local", "--subdir", "DAGS_FOLDER/etl_distributed.py"]
#ENV AIRFLOW__CORE__EXECUTOR="LocalExecutor"
#ENV AIRFLOW_IS_K8S_EXECUTOR_POD="True"
