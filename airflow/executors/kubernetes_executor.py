# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
KubernetesExecutor

.. seealso::
    For more information on how the KubernetesExecutor works, take a look at the guide:
    :ref:`executor:KubernetesExecutor`
"""
from __future__ import annotations

import collections
import concurrent.futures
import functools
import json
import logging
import multiprocessing
import subprocess
import time
from datetime import timedelta
from queue import Empty, Queue
from typing import Any, Dict, Optional, Sequence, Tuple, List

import requests

from airflow.models.dagbag import DagBag

from airflow.models.dag import DagModel
from kubernetes import client, watch
from kubernetes.client import Configuration, models as k8s
from kubernetes.client.rest import ApiException
from urllib3.exceptions import ReadTimeoutError

from airflow.exceptions import AirflowException, PodReconciliationError
from airflow.executors.base_executor import NOT_STARTED_MESSAGE, BaseExecutor, CommandType
from airflow.kubernetes import pod_generator
from airflow.kubernetes.kube_client import get_kube_client
from airflow.kubernetes.kube_config import KubeConfig
from airflow.kubernetes.kubernetes_helper_functions import annotations_to_key, create_pod_id
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.settings import pod_mutation_hook
from airflow.utils import timezone
from airflow.utils.event_scheduler import EventScheduler
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.state import State

# Ours
from airflow.grpc.connector import Connector as grpc_connector

# TaskInstance key, command, configuration, pod_template_file
KubernetesJobType = Tuple[TaskInstanceKey, CommandType, Any, Optional[str]]

# key, state, pod_id, namespace, resource_version
KubernetesResultsType = Tuple[TaskInstanceKey, Optional[str], str, str, str]

# pod_id, namespace, state, annotations, resource_version
KubernetesWatchType = Tuple[str, str, Optional[str], Dict[str, str], str]


class ResourceVersion:
    """Singleton for tracking resourceVersion from Kubernetes"""

    _instance = None
    resource_version = "0"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance


class KubernetesJobWatcher(multiprocessing.Process, LoggingMixin):
    """Watches for Kubernetes jobs"""

    def __init__(
        self,
        namespace: str | None,
        multi_namespace_mode: bool,
        watcher_queue: Queue[KubernetesWatchType],
        resource_version: str | None,
        scheduler_job_id: str,
        kube_config: Configuration,
    ):
        super().__init__()
        self.namespace = namespace
        self.multi_namespace_mode = multi_namespace_mode
        self.scheduler_job_id = scheduler_job_id
        self.watcher_queue = watcher_queue
        self.resource_version = resource_version
        self.kube_config = kube_config

    def run(self) -> None:
        """Performs watching"""
        kube_client: client.CoreV1Api = get_kube_client()
        if not self.scheduler_job_id:
            raise AirflowException(NOT_STARTED_MESSAGE)
        while True:
            try:
                self.resource_version = self._run(
                    kube_client, self.resource_version, self.scheduler_job_id, self.kube_config
                )
            except ReadTimeoutError:
                self.log.warning(
                    "There was a timeout error accessing the Kube API. Retrying request.", exc_info=True
                )
                time.sleep(1)
            except Exception:
                self.log.exception('Unknown error in KubernetesJobWatcher. Failing')
                self.resource_version = "0"
                ResourceVersion().resource_version = "0"
                raise
            else:
                self.log.warning(
                    'Watch died gracefully, starting back up with: last resource_version: %s',
                    self.resource_version,
                )

    def _run(
        self,
        kube_client: client.CoreV1Api,
        resource_version: str | None,
        scheduler_job_id: str,
        kube_config: Any,
    ) -> str | None:
        self.log.info('Event: and now my watch begins starting at resource_version: %s', resource_version)
        watcher = watch.Watch()

        kwargs = {'label_selector': f'airflow-worker={scheduler_job_id}'}
        if resource_version:
            kwargs['resource_version'] = resource_version
        if kube_config.kube_client_request_args:
            for key, value in kube_config.kube_client_request_args.items():
                kwargs[key] = value

        last_resource_version: str | None = None
        if self.multi_namespace_mode:
            list_worker_pods = functools.partial(
                watcher.stream, kube_client.list_pod_for_all_namespaces, **kwargs
            )
        else:
            list_worker_pods = functools.partial(
                watcher.stream, kube_client.list_namespaced_pod, self.namespace, **kwargs
            )
        for event in list_worker_pods():
            task = event['object']
            self.log.info('Event: %s had an event of type %s', task.metadata.name, event['type'])
            if event['type'] == 'ERROR':
                return self.process_error(event)
            annotations = task.metadata.annotations
            task_instance_related_annotations = {
                'dag_id': annotations['dag_id'],
                'task_id': annotations['task_id'],
                'execution_date': annotations.get('execution_date'),
                'run_id': annotations.get('run_id'),
                'try_number': annotations['try_number'],
            }
            map_index = annotations.get('map_index')
            if map_index is not None:
                task_instance_related_annotations['map_index'] = map_index

            self.process_status(
                pod_id=task.metadata.name,
                namespace=task.metadata.namespace,
                status=task.status.phase,
                annotations=task_instance_related_annotations,
                resource_version=task.metadata.resource_version,
                event=event,
            )
            last_resource_version = task.metadata.resource_version

        return last_resource_version

    def process_error(self, event: Any) -> str:
        """Process error response"""
        self.log.error('Encountered Error response from k8s list namespaced pod stream => %s', event)
        raw_object = event['raw_object']
        if raw_object['code'] == 410:
            self.log.info(
                'Kubernetes resource version is too old, must reset to 0 => %s', (raw_object['message'],)
            )
            # Return resource version 0
            return '0'
        raise AirflowException(
            f"Kubernetes failure for {raw_object['reason']} with code {raw_object['code']} and message: "
            f"{raw_object['message']}"
        )

    def process_status(
        self,
        pod_id: str,
        namespace: str,
        status: str,
        annotations: dict[str, str],
        resource_version: str,
        event: Any,
    ) -> None:
        """Process status response"""
        if status == 'Pending':
            if event['type'] == 'DELETED':
                self.log.info('Event: Failed to start pod %s', pod_id)
                self.watcher_queue.put((pod_id, namespace, State.FAILED, annotations, resource_version))
            else:
                self.log.info('Event: %s Pending', pod_id)
        elif status == 'Failed':
            self.log.error('Event: %s Failed', pod_id)
            self.watcher_queue.put((pod_id, namespace, State.FAILED, annotations, resource_version))
        elif status == 'Succeeded':
            self.log.info('Event: %s Succeeded', pod_id)
            self.watcher_queue.put((pod_id, namespace, None, annotations, resource_version))
        elif status == 'Running':
            if event['type'] == 'DELETED':
                self.log.info('Event: Pod %s deleted before it could complete', pod_id)
                self.watcher_queue.put((pod_id, namespace, State.FAILED, annotations, resource_version))
            else:
                self.log.info('Event: %s is Running', pod_id)
        else:
            self.log.warning(
                'Event: Invalid state: %s on pod: %s in namespace %s with annotations: %s with '
                'resource_version: %s',
                status,
                pod_id,
                namespace,
                annotations,
                resource_version,
            )


TaskExecutionKey = collections.namedtuple("TaskExecutionKey", ("task_id", "dag_id", "run_id"))

class Timer:
    def __init__(self, func_name, annotations):
        self._times = []
        self._timestamp_annotations = []
        self._func_name = func_name
        self._annotations = annotations

    def time(self, annotation: str):
        self._times.append(time.time())
        self._timestamp_annotations.append(annotation)

    def get_log_line(self):
        timing_info = {"function": self._func_name, "times": self._times, "timestamp_annotations": self._timestamp_annotations}
        timing_info.update(self._annotations)
        return f'TIMING: {json.dumps(timing_info)}'

    def update_annotations(self, new_annotations):
        self._annotations.update(new_annotations)

class AirflowKubernetesScheduler(LoggingMixin):
    """Airflow Scheduler for Kubernetes"""

    def __init__(
        self,
        kube_config: Any,
        task_queue: Queue[KubernetesJobType],
        result_queue: Queue[KubernetesResultsType],
        kube_client: client.CoreV1Api,
        scheduler_job_id: str,
    ):
        super().__init__()
        self.log.debug("Creating Kubernetes executor")
        self.kube_config = kube_config
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.namespace = self.kube_config.kube_namespace
        self.log.debug("Kubernetes using namespace %s", self.namespace)
        self.kube_client = kube_client
        self._manager = multiprocessing.Manager()
        self.watcher_queue = self._manager.Queue()
        self.scheduler_job_id = scheduler_job_id
        self.kube_watcher = self._make_kube_watcher()
        self.executor_pool = concurrent.futures.ThreadPoolExecutor()
        self._kn_workers: Dict[Tuple[str, str], str] | None = None  # map dag_id to knative worker urls

        # load kn service urls asynchronously
        self._worker_service_urls_future = self.executor_pool.submit(self._retrieve_kn_service_urls)

        self.xcoms: Dict[TaskExecutionKey, List[concurrent.futures.Future]] = collections.defaultdict(lambda: [])

    def _retrieve_kn_service_urls(self):
        while True:
            kn = subprocess.run(('kn', 'service', 'list', '-n', 'airflow', '-o', 'json'), stdout=subprocess.PIPE)
            if kn.returncode != 0:
                self.log.warning(f'kn service list exited with code {kn.returncode}, retrying in 1 second')
                time.sleep(1)
                continue

            self.log.info(f'kn service list returned')
            knative_services = json.loads(kn.stdout)
            self.log.debug(f'knative_services: {knative_services}')

            kn_workers = {}
            for service in knative_services['items']:
                try:
                    dag_id = service['metadata']['annotations']['dag_id']
                    task_id = service['metadata']['annotations']['task_id']
                    worker_url = service['status']['url']
                    kn_workers[(dag_id, task_id)] = worker_url
                except KeyError as e:
                    logging.info(e)
            return kn_workers

    def get_worker_service_url(self, dag_id: str, task_id: str):
        # when this method is called for the first time the worker service urls should already have been
        # loaded asynchronously, so we just retrieve and store them
        if self._kn_workers is None:
            try:
                self._kn_workers = self._worker_service_urls_future.result(timeout=20)
            except TimeoutError:
                self.log.error(f'Timeout while waiting for list of knative services')
                return None
        if (dag_id, task_id) not in self._kn_workers:
            # maybe worker for this dag was not yet available when we last checked, so try again
            self._worker_service_urls_future = self.executor_pool.submit(self._retrieve_kn_service_urls)
            self._kn_workers = self._worker_service_urls_future.result(timeout=20)

        return self._kn_workers.get((dag_id, task_id))

    def run_pod_async(self, pod: k8s.V1Pod, command, key, custom_annotations, **kwargs):
        run_pod_timer = Timer("executor_run_pod_async", custom_annotations)
        run_pod_timer.time("function_entry")
        """Runs POD asynchronously"""
        pod_mutation_hook(pod)

        sanitized_pod = self.kube_client.api_client.sanitize_for_serialization(pod)
        json_pod = json.dumps(sanitized_pod, indent=2)
        current_task_id = custom_annotations["task_id"]
        dag_id = custom_annotations["dag_id"]

        worker_service_url = self.get_worker_service_url(dag_id, current_task_id)
        if worker_service_url is None:
            self.log.error(f'No knative worker available for task {current_task_id} in dag {dag_id}')
            self.watcher_queue.put(("airflow-worker-0", "airflow", State.FAILED, custom_annotations, 0))
            return

        # endpoint = worker_service_url + "/run_task_instance"
        endpoint = worker_service_url
        self.log.info(f'Knative service airflow worker endpoint: {endpoint}')

        self.log.debug('Pod Creation Request: \n%s', json_pod)

        run_id = custom_annotations["run_id"]
        current_task_execution_key = TaskExecutionKey(current_task_id, dag_id, run_id)
        try:
            dagbag = DagBag()
            dag = dagbag.get_dag(dag_id)
            current_task = dag.task_dict[current_task_id]
        except Exception as e:
            self.log.warning(e)

        def task(endpoint: str, args: List[str], watcher_queue, log):
            # collect xcom values from upstream tasks
            timer = Timer("executor_async_task", custom_annotations)
            timer.time("function_entry")
            try:
                xcoms = []
                for upstream_task_id in current_task.upstream_task_ids:
                    dependency_key = TaskExecutionKey(upstream_task_id, dag_id, run_id)
                    try:
                        for future in self.xcoms[dependency_key]:
                            # this timeout can be fairly short because dependencies should be finished already
                            xcom = future.result(timeout=5)
                            xcoms.extend(xcom)
                    except KeyError:
                        log.warning(f"Did not find an xcom (return) value for {dependency_key}")
                    except TimeoutError as e:
                        log.error(e)
                        watcher_queue.put(("airflow-worker-0", "airflow", State.FAILED, custom_annotations, 0))
                        return

                log.info(f"Sending POST request to {endpoint} with arguments {args}, xcoms {xcoms} and annotation {custom_annotations}")
                timer.time("before_post_request")
                # r = requests.post(endpoint, json={"args": args, "xcoms": xcoms, "annotations": custom_annotations}).json()
                log.info(f"Call grpc_connector!")
                r = grpc_connector.send( endpoint, payloads={
                    "args": args,
                    "xcoms": xcoms,
                    "annotations": custom_annotations,
                })
                log.info(f"After sending messages to worker via grpc_connector!")
                timer.time("after_post_request")

                log.info(f'Task run {custom_annotations["run_id"]} done with status code {r["status_code"]}. Data: {r}')
                # data = r.json()
                data = r
                logging.info(f'response: {data}')

                if r["status_code"] == "UNKNOWN":
                    # state 'None' indicates success in this context
                    watcher_queue.put(("airflow-worker-0", "airflow", None, custom_annotations, 0))
                    timer.time("function_exit")
                    self.log.info(timer.get_log_line())
                    # self.log.info(f'TIMING: {json.dumps(data["timing_info"])}')  # dump timing info from worker
                    return data["xcoms"]
                else:
                    watcher_queue.put(("airflow-worker-0", "airflow", State.FAILED, custom_annotations, 0))
                    return
            except Exception as e:
                log.error(e)
                watcher_queue.put(("airflow-worker-0", "airflow", State.FAILED, custom_annotations, 0))
                return

        self.xcoms[current_task_execution_key].append(self.executor_pool.submit(task, endpoint, command, self.watcher_queue, self.log))
        self.log.info(f'Submitted {current_task_execution_key} to executor pool')
        run_pod_timer.time("function_exit")
        self.log.info(run_pod_timer.get_log_line())

    def _make_kube_watcher(self) -> KubernetesJobWatcher:
        resource_version = ResourceVersion().resource_version
        watcher = KubernetesJobWatcher(
            watcher_queue=self.watcher_queue,
            namespace=self.kube_config.kube_namespace,
            multi_namespace_mode=self.kube_config.multi_namespace_mode,
            resource_version=resource_version,
            scheduler_job_id=self.scheduler_job_id,
            kube_config=self.kube_config,
        )
        watcher.start()
        return watcher

    def _health_check_kube_watcher(self):
        if self.kube_watcher.is_alive():
            self.log.debug("KubeJobWatcher alive, continuing")
        else:
            self.log.error(
                'Error while health checking kube watcher process. Process died for unknown reasons'
            )
            ResourceVersion().resource_version = "0"
            self.kube_watcher = self._make_kube_watcher()

    def run_next(self, next_job: KubernetesJobType) -> None:
        """
        The run_next command will check the task_queue for any un-run jobs.
        It will then create a unique job-id, launch that job in the cluster,
        and store relevant info in the current_jobs map so we can track the job's
        status
        """
        key, command, kube_executor_config, pod_template_file = next_job
        self.log.info('Kubernetes job is %s', key)

        dag_id, task_id, run_id, try_number, map_index = key

        if command[0:3] != ["airflow", "tasks", "run"]:
            raise ValueError('The command must start with ["airflow", "tasks", "run"].')

        base_worker_pod = get_base_pod_from_template(pod_template_file, self.kube_config)

        if not base_worker_pod:
            raise AirflowException(
                f"could not find a valid worker template yaml at {self.kube_config.pod_template_file}"
            )

        pod = PodGenerator.construct_pod(
            namespace=self.namespace,
            scheduler_job_id=self.scheduler_job_id,
            pod_id=create_pod_id(dag_id, task_id),
            dag_id=dag_id,
            task_id=task_id,
            kube_image=self.kube_config.kube_image,
            try_number=try_number,
            map_index=map_index,
            date=None,
            run_id=run_id,
            args=command,
            pod_override_object=kube_executor_config,
            base_worker_pod=base_worker_pod,
        )
        custom_annotations = {
            'dag_id': dag_id,
            'task_id': task_id,
            'try_number': try_number,
            'run_id': run_id,
            'map_index': map_index,
        }
        # Reconcile the pod generated by the Operator and the Pod
        # generated by the .cfg file
        self.log.debug("Kubernetes running for command %s", command)
        self.log.debug("Kubernetes launching image %s", pod.spec.containers[0].image)

        # the watcher will monitor pods, so we do not block.
        self.run_pod_async(pod, command, key, custom_annotations, **self.kube_config.kube_client_request_args)
        self.log.debug("Kubernetes Job created!")

    def delete_pod(self, pod_id: str, namespace: str) -> None:
        """Deletes POD"""
        try:
            self.log.debug("Deleting pod %s in namespace %s", pod_id, namespace)
            self.kube_client.delete_namespaced_pod(
                pod_id,
                namespace,
                body=client.V1DeleteOptions(**self.kube_config.delete_option_kwargs),
                **self.kube_config.kube_client_request_args,
            )
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    def sync(self) -> None:
        """
        The sync function checks the status of all currently running kubernetes jobs.
        If a job is completed, its status is placed in the result queue to
        be sent back to the scheduler.

        :return:

        """
        self.log.debug("Syncing KubernetesExecutor")
        self._health_check_kube_watcher()
        while True:
            try:
                task = self.watcher_queue.get_nowait()
                try:
                    self.log.debug("Processing task %s", task)
                    self.process_watcher_task(task)
                finally:
                    self.watcher_queue.task_done()
            except Empty:
                break

    def process_watcher_task(self, task: KubernetesWatchType) -> None:
        """Process the task by watcher."""
        pod_id, namespace, state, annotations, resource_version = task
        self.log.info(
            'Attempting to finish pod; pod_id: %s; state: %s; annotations: %s', pod_id, state, annotations
        )
        key = annotations_to_key(annotations=annotations)
        if key:
            self.log.debug('finishing job %s - %s (%s)', key, state, pod_id)
            self.result_queue.put((key, state, pod_id, namespace, resource_version))

    def _flush_watcher_queue(self) -> None:
        self.log.debug('Executor shutting down, watcher_queue approx. size=%d', self.watcher_queue.qsize())
        while True:
            try:
                task = self.watcher_queue.get_nowait()
                # Ignoring it since it can only have either FAILED or SUCCEEDED pods
                self.log.warning('Executor shutting down, IGNORING watcher task=%s', task)
                self.watcher_queue.task_done()
            except Empty:
                break

    def terminate(self) -> None:
        """Terminates the watcher."""
        self.log.debug("Terminating kube_watcher...")
        self.kube_watcher.terminate()
        self.kube_watcher.join()
        self.log.debug("kube_watcher=%s", self.kube_watcher)
        self.log.debug("Flushing watcher_queue...")
        self._flush_watcher_queue()
        # Queue should be empty...
        self.watcher_queue.join()
        self.log.debug("Shutting down manager...")
        self._manager.shutdown()


def get_base_pod_from_template(pod_template_file: str | None, kube_config: Any) -> k8s.V1Pod:
    """
    Reads either the pod_template_file set in the executor_config or the base pod_template_file
    set in the airflow.cfg to craft a "base pod" that will be used by the KubernetesExecutor

    :param pod_template_file: absolute path to a pod_template_file.yaml or None
    :param kube_config: The KubeConfig class generated by airflow that contains all kube metadata
    :return: a V1Pod that can be used as the base pod for k8s tasks
    """
    if pod_template_file:
        return PodGenerator.deserialize_model_file(pod_template_file)
    else:
        return PodGenerator.deserialize_model_file(kube_config.pod_template_file)


class KubernetesExecutor(BaseExecutor):
    """Executor for Kubernetes"""

    supports_ad_hoc_ti_run: bool = True

    def __init__(self):
        self.kube_config = KubeConfig()
        self._manager = multiprocessing.Manager()
        self.task_queue: Queue[KubernetesJobType] = self._manager.Queue()
        self.result_queue: Queue[KubernetesResultsType] = self._manager.Queue()
        self.kube_scheduler: AirflowKubernetesScheduler | None = None
        self.kube_client: client.CoreV1Api | None = None
        self.scheduler_job_id: str | None = None
        self.event_scheduler: EventScheduler | None = None
        self.last_handled: dict[TaskInstanceKey, float] = {}
        self.kubernetes_queue: str | None = None
        super().__init__(parallelism=self.kube_config.parallelism)

    @provide_session
    def clear_not_launched_queued_tasks(self, session=None) -> None:
        """
        Tasks can end up in a "Queued" state through either the executor being
        abruptly shut down (leaving a non-empty task_queue on this executor)
        or when a rescheduled/deferred operator comes back up for execution
        (with the same try_number) before the pod of its previous incarnation
        has been fully removed (we think).

        This method checks each of those tasks to see if the corresponding pod
        is around, and if not, and there's no matching entry in our own
        task_queue, marks it for re-execution.
        """
        self.log.debug("Clearing tasks that have not been launched")
        if not self.kube_client:
            raise AirflowException(NOT_STARTED_MESSAGE)

        query = session.query(TaskInstance).filter(TaskInstance.state == State.QUEUED)
        if self.kubernetes_queue:
            query = query.filter(TaskInstance.queue == self.kubernetes_queue)
        queued_tis: list[TaskInstance] = query.all()
        self.log.info('Found %s queued task instances', len(queued_tis))

        # Go through the "last seen" dictionary and clean out old entries
        allowed_age = self.kube_config.worker_pods_queued_check_interval * 3
        for key, timestamp in list(self.last_handled.items()):
            if time.time() - timestamp > allowed_age:
                del self.last_handled[key]

        for ti in queued_tis:
            self.log.debug("Checking task instance %s", ti)

            # Check to see if we've handled it ourselves recently
            if ti.key in self.last_handled:
                continue

            # Build the pod selector
            base_label_selector = (
                f"dag_id={pod_generator.make_safe_label_value(ti.dag_id)},"
                f"task_id={pod_generator.make_safe_label_value(ti.task_id)},"
                f"airflow-worker={pod_generator.make_safe_label_value(str(ti.queued_by_job_id))}"
            )
            if ti.map_index >= 0:
                # Old tasks _couldn't_ be mapped, so we don't have to worry about compat
                base_label_selector += f',map_index={ti.map_index}'
            kwargs = dict(label_selector=base_label_selector)
            if self.kube_config.kube_client_request_args:
                kwargs.update(**self.kube_config.kube_client_request_args)

            # Try run_id first
            kwargs['label_selector'] += ',run_id=' + pod_generator.make_safe_label_value(ti.run_id)
            pod_list = self.kube_client.list_namespaced_pod(self.kube_config.kube_namespace, **kwargs)
            if pod_list.items:
                continue
            # Fallback to old style of using execution_date
            kwargs['label_selector'] = (
                f'{base_label_selector},'
                f'execution_date={pod_generator.datetime_to_label_safe_datestring(ti.execution_date)}'
            )
            pod_list = self.kube_client.list_namespaced_pod(self.kube_config.kube_namespace, **kwargs)
            if pod_list.items:
                continue
            self.log.info('TaskInstance: %s found in queued state but was not launched, rescheduling', ti)
            session.query(TaskInstance).filter(
                TaskInstance.dag_id == ti.dag_id,
                TaskInstance.task_id == ti.task_id,
                TaskInstance.run_id == ti.run_id,
                TaskInstance.map_index == ti.map_index,
            ).update({TaskInstance.state: State.SCHEDULED})

    def start(self) -> None:
        """Starts the executor"""
        self.log.info('Start Kubernetes executor')
        if not self.job_id:
            raise AirflowException("Could not get scheduler_job_id")
        self.scheduler_job_id = str(self.job_id)
        self.log.debug('Start with scheduler_job_id: %s', self.scheduler_job_id)
        self.kube_client = get_kube_client()
        self.kube_scheduler = AirflowKubernetesScheduler(
            self.kube_config, self.task_queue, self.result_queue, self.kube_client, self.scheduler_job_id
        )
        self.event_scheduler = EventScheduler()
        self.event_scheduler.call_regular_interval(
            self.kube_config.worker_pods_pending_timeout_check_interval,
            self._check_worker_pods_pending_timeout,
        )
        self.event_scheduler.call_regular_interval(
            self.kube_config.worker_pods_queued_check_interval,
            self.clear_not_launched_queued_tasks,
        )
        # We also call this at startup as that's the most likely time to see
        # stuck queued tasks
        self.clear_not_launched_queued_tasks()

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:
        """Executes task asynchronously"""
        self.log.info('Add task %s with command %s with executor_config %s', key, command, executor_config)
        try:
            kube_executor_config = PodGenerator.from_obj(executor_config)
        except Exception:
            self.log.error("Invalid executor_config for %s", key)
            self.fail(key=key, info="Invalid executor_config passed")
            return

        if executor_config:
            pod_template_file = executor_config.get("pod_template_file", None)
        else:
            pod_template_file = None
        if not self.task_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.event_buffer[key] = (State.QUEUED, self.scheduler_job_id)
        self.task_queue.put((key, command, kube_executor_config, pod_template_file))
        # We keep a temporary local record that we've handled this so we don't
        # try and remove it from the QUEUED state while we process it
        self.last_handled[key] = time.time()

    def sync(self) -> None:
        """Synchronize task state."""
        if self.running:
            self.log.debug('self.running: %s', self.running)
        if self.queued_tasks:
            self.log.debug('self.queued: %s', self.queued_tasks)
        if not self.scheduler_job_id:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.kube_scheduler:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.kube_config:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.result_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.task_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.event_scheduler:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.kube_scheduler.sync()

        last_resource_version = None
        while True:
            try:
                results = self.result_queue.get_nowait()
                try:
                    key, state, pod_id, namespace, resource_version = results
                    last_resource_version = resource_version
                    self.log.info('Changing state of %s to %s', results, state)
                    try:
                        self._change_state(key, state, pod_id, namespace)
                    except Exception as e:
                        self.log.exception(
                            "Exception: %s when attempting to change state of %s to %s, re-queueing.",
                            e,
                            results,
                            state,
                        )
                        self.result_queue.put(results)
                finally:
                    self.result_queue.task_done()
            except Empty:
                break

        resource_instance = ResourceVersion()
        resource_instance.resource_version = last_resource_version or resource_instance.resource_version

        for _ in range(self.kube_config.worker_pods_creation_batch_size):
            try:
                task = self.task_queue.get_nowait()
                try:
                    self.kube_scheduler.run_next(task)
                except PodReconciliationError as e:
                    self.log.error(
                        "Pod reconciliation failed, likely due to kubernetes library upgrade. "
                        "Try clearing the task to re-run.",
                        exc_info=True,
                    )
                    self.fail(task[0], e)
                except ApiException as e:

                    # These codes indicate something is wrong with pod definition; otherwise we assume pod
                    # definition is ok, and that retrying may work
                    if e.status in (400, 422):
                        self.log.error("Pod creation failed with reason %r. Failing task", e.reason)
                        key, _, _, _ = task
                        self.change_state(key, State.FAILED, e)
                    else:
                        self.log.warning(
                            'ApiException when attempting to run task, re-queueing. Reason: %r. Message: %s',
                            e.reason,
                            json.loads(e.body)['message'],
                        )
                        self.task_queue.put(task)
                finally:
                    self.task_queue.task_done()
            except Empty:
                break

        # Run any pending timed events
        next_event = self.event_scheduler.run(blocking=False)
        self.log.debug("Next timed event is in %f", next_event)

    def _check_worker_pods_pending_timeout(self):
        """Check if any pending worker pods have timed out"""
        if not self.scheduler_job_id:
            raise AirflowException(NOT_STARTED_MESSAGE)
        timeout = self.kube_config.worker_pods_pending_timeout
        self.log.debug('Looking for pending worker pods older than %d seconds', timeout)

        kwargs = {
            'limit': self.kube_config.worker_pods_pending_timeout_batch_size,
            'field_selector': 'status.phase=Pending',
            'label_selector': f'airflow-worker={self.scheduler_job_id}',
            **self.kube_config.kube_client_request_args,
        }
        if self.kube_config.multi_namespace_mode:
            pending_pods = functools.partial(self.kube_client.list_pod_for_all_namespaces, **kwargs)
        else:
            pending_pods = functools.partial(
                self.kube_client.list_namespaced_pod, self.kube_config.kube_namespace, **kwargs
            )

        cutoff = timezone.utcnow() - timedelta(seconds=timeout)
        for pod in pending_pods().items:
            self.log.debug(
                'Found a pending pod "%s", created "%s"', pod.metadata.name, pod.metadata.creation_timestamp
            )
            if pod.metadata.creation_timestamp < cutoff:
                self.log.error(
                    (
                        'Pod "%s" has been pending for longer than %d seconds.'
                        'It will be deleted and set to failed.'
                    ),
                    pod.metadata.name,
                    timeout,
                )
                self.kube_scheduler.delete_pod(pod.metadata.name, pod.metadata.namespace)

    def _change_state(self, key: TaskInstanceKey, state: str | None, pod_id: str, namespace: str) -> None:
        if state != State.RUNNING:
            if self.kube_config.delete_worker_pods:
                if not self.kube_scheduler:
                    raise AirflowException(NOT_STARTED_MESSAGE)
                if state != State.FAILED or self.kube_config.delete_worker_pods_on_failure:
                    # self.kube_scheduler.delete_pod(pod_id, namespace)
                    self.log.info('Would delete pod: %s in namespace %s', str(key), str(namespace))
            try:
                self.running.remove(key)
            except KeyError:
                self.log.debug('Could not find key: %s', str(key))
        self.event_buffer[key] = state, None

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        tis_to_flush = [ti for ti in tis if not ti.queued_by_job_id]
        scheduler_job_ids = {ti.queued_by_job_id for ti in tis}
        pod_ids = {ti.key: ti for ti in tis if ti.queued_by_job_id}
        kube_client: client.CoreV1Api = self.kube_client
        for scheduler_job_id in scheduler_job_ids:
            scheduler_job_id = pod_generator.make_safe_label_value(str(scheduler_job_id))
            kwargs = {'label_selector': f'airflow-worker={scheduler_job_id}'}
            pod_list = kube_client.list_namespaced_pod(namespace=self.kube_config.kube_namespace, **kwargs)
            for pod in pod_list.items:
                self.adopt_launched_task(kube_client, pod, pod_ids)
        self._adopt_completed_pods(kube_client)
        tis_to_flush.extend(pod_ids.values())
        return tis_to_flush

    def adopt_launched_task(
        self, kube_client: client.CoreV1Api, pod: k8s.V1Pod, pod_ids: dict[TaskInstanceKey, k8s.V1Pod]
    ) -> None:
        """
        Patch existing pod so that the current KubernetesJobWatcher can monitor it via label selectors

        :param kube_client: kubernetes client for speaking to kube API
        :param pod: V1Pod spec that we will patch with new label
        :param pod_ids: pod_ids we expect to patch.
        """
        if not self.scheduler_job_id:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.log.info("attempting to adopt pod %s", pod.metadata.name)
        pod.metadata.labels['airflow-worker'] = pod_generator.make_safe_label_value(self.scheduler_job_id)
        pod_id = annotations_to_key(pod.metadata.annotations)
        if pod_id not in pod_ids:
            self.log.error("attempting to adopt taskinstance which was not specified by database: %s", pod_id)
            return

        try:
            kube_client.patch_namespaced_pod(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                body=PodGenerator.serialize_pod(pod),
            )
            pod_ids.pop(pod_id)
            self.running.add(pod_id)
        except ApiException as e:
            self.log.info("Failed to adopt pod %s. Reason: %s", pod.metadata.name, e)

    def _adopt_completed_pods(self, kube_client: client.CoreV1Api) -> None:
        """

        Patch completed pod so that the KubernetesJobWatcher can delete it.

        :param kube_client: kubernetes client for speaking to kube API
        """
        if not self.scheduler_job_id:
            raise AirflowException(NOT_STARTED_MESSAGE)
        new_worker_id_label = pod_generator.make_safe_label_value(self.scheduler_job_id)
        kwargs = {
            'field_selector': "status.phase=Succeeded",
            'label_selector': f'kubernetes_executor=True,airflow-worker!={new_worker_id_label}',
        }
        pod_list = kube_client.list_namespaced_pod(namespace=self.kube_config.kube_namespace, **kwargs)
        for pod in pod_list.items:
            self.log.info("Attempting to adopt pod %s", pod.metadata.name)
            pod.metadata.labels['airflow-worker'] = new_worker_id_label
            try:
                kube_client.patch_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    body=PodGenerator.serialize_pod(pod),
                )
            except ApiException as e:
                self.log.info("Failed to adopt pod %s. Reason: %s", pod.metadata.name, e)

    def _flush_task_queue(self) -> None:
        if not self.task_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.log.debug('Executor shutting down, task_queue approximate size=%d', self.task_queue.qsize())
        while True:
            try:
                task = self.task_queue.get_nowait()
                # This is a new task to run thus ok to ignore.
                self.log.warning('Executor shutting down, will NOT run task=%s', task)
                self.task_queue.task_done()
            except Empty:
                break

    def _flush_result_queue(self) -> None:
        if not self.result_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.log.debug('Executor shutting down, result_queue approximate size=%d', self.result_queue.qsize())
        while True:
            try:
                results = self.result_queue.get_nowait()
                self.log.warning('Executor shutting down, flushing results=%s', results)
                try:
                    key, state, pod_id, namespace, resource_version = results
                    self.log.info(
                        'Changing state of %s to %s : resource_version=%d', results, state, resource_version
                    )
                    try:
                        self._change_state(key, state, pod_id, namespace)
                    except Exception as e:
                        self.log.exception(
                            'Ignoring exception: %s when attempting to change state of %s to %s.',
                            e,
                            results,
                            state,
                        )
                finally:
                    self.result_queue.task_done()
            except Empty:
                break

    def end(self) -> None:
        """Called when the executor shuts down"""
        if not self.task_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.result_queue:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.kube_scheduler:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.log.info('Shutting down Kubernetes executor')
        self.log.debug('Flushing task_queue...')
        self._flush_task_queue()
        self.log.debug('Flushing result_queue...')
        self._flush_result_queue()
        # Both queues should be empty...
        self.task_queue.join()
        self.result_queue.join()
        if self.kube_scheduler:
            self.kube_scheduler.terminate()
        self._manager.shutdown()

    def terminate(self):
        """Terminate the executor is not doing anything."""
