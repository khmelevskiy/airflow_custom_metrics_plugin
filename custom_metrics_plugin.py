import logging

from airflow.listeners import hookimpl
from airflow.models import TaskInstance
from airflow.plugins_manager import AirflowPlugin
from airflow.stats import Stats
from airflow.utils.state import TaskInstanceState


STOPPED_TASK_INSTANCES_STATUSES = {
    TaskInstanceState.REMOVED,
    TaskInstanceState.RESTARTING,
    TaskInstanceState.UP_FOR_RETRY,
    TaskInstanceState.UP_FOR_RESCHEDULE,
    TaskInstanceState.UPSTREAM_FAILED,
    TaskInstanceState.SKIPPED,
    TaskInstanceState.DEFERRED,
}  # First added for the SKIPPED status, because airflow just terminated the process, and we should reset the metric.
# The rest of the statuses are for insurance, nothing should break.


def convert_bytes_to_readable_value(size_bytes: int) -> str:
    """
    Convert bytes to human readable value. Max supported value is '1023.99 PB'.

    Parameters
    ----------
    size_bytes: int
        Size in bytes.

    Returns
    -------
    str
        Human readable value
    """

    import math

    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)

    return f"{s} {size_name[i]}"


class TaskMonitorListener:
    def __init__(self):
        self.monitor_threads = {}

    @property
    def check_plugin_enabled(self):
        """
        If the plugin is disabled via an environment variable or if the environment variable is not set, we exit
        """

        import os

        return os.getenv("CUSTOM_METRICS_PLUGIN_ENABLED", "False").lower() in ["true", "1", "t", "y", "yes"]

    def get_metric_name(self, task_instance: TaskInstance, metric_name: str) -> str:
        """
        Get metric name for task_instance.
        In statsd configuration they have name: `tasks_cpu_usage_percent` and `tasks_memory_usage_bytes`.
        Replace `.` to `__` in dag_id and task_id, because with `.` in name metrics are not displayed in Grafana.

        Parameters
        ----------
        task_instance: TaskInstance
            Task instance object.
        metric_name: str
            Metric name.

        Returns
        -------
        str
            Formatted metric name.
        """
        return f"{metric_name}.{str(task_instance.dag_id).replace('.',"__")}.{str(task_instance.task_id).replace('.',"__")}"

    def _finalize_task(self, task_instance: TaskInstance) -> None:
        """
        Finalize task monitoring. Stop monitoring thread and set metrics to 0.

        Parameters
        ----------
        task_instance: TaskInstance
            Task instance object.
        """

        if not self.check_plugin_enabled:
            return

        if task_instance in self.monitor_threads:
            monitor_info = self.monitor_threads.pop(task_instance, {})
            if monitor_info:
                monitor_thread, stop_monitoring = monitor_info
                stop_monitoring.set()
                if monitor_thread.is_alive():
                    monitor_thread.join()

            Stats.gauge(self.get_metric_name(task_instance, "custom_metrics_cpu_usage_percent"), 0)
            Stats.gauge(self.get_metric_name(task_instance, "custom_metrics_memory_usage_bytes"), 0)

            logging.info(
                f"===== 📊🏁CUSTOM_METRICS_PLUGIN. Task {task_instance.task_id} in DAG {task_instance.dag_id} completed ====="
            )

    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance, session, **kwargs):
        if not self.check_plugin_enabled:
            return

        import os
        import threading
        import time

        import psutil

        sleep_interval_seconds = int(os.getenv("CUSTOM_METRICS_PLUGIN_SLEEP_INTERVAL_SECONDS", 1))

        logging.info(
            f"===== 📊▶️CUSTOM_METRICS_PLUGIN. Task {task_instance.task_id} in DAG {task_instance.dag_id} is starting ====="
        )

        process = psutil.Process(task_instance.pid)
        stop_monitoring = threading.Event()

        def monitor():
            while not stop_monitoring.is_set():
                if not process.is_running() or task_instance.state in STOPPED_TASK_INSTANCES_STATUSES:
                    break
                memory_usage = process.memory_info().rss  # Used memory by process by bytes
                cpu_usage = process.cpu_percent(interval=1)  # CPU usage by process in percentage

                Stats.gauge(self.get_metric_name(task_instance, "custom_metrics_cpu_usage_percent"), cpu_usage)
                Stats.gauge(self.get_metric_name(task_instance, "custom_metrics_memory_usage_bytes"), memory_usage)

                logging.info(
                    f"===== 📊CUSTOM_METRICS_PLUGIN. Memory usage: {convert_bytes_to_readable_value(memory_usage)}, CPU usage: {cpu_usage:.2f}% ====="
                )
                time.sleep(sleep_interval_seconds)

        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()

        self.monitor_threads[task_instance] = (monitor_thread, stop_monitoring)

    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance, session, **kwargs):
        self._finalize_task(task_instance)

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance, **kwargs):
        self._finalize_task(task_instance)

    @hookimpl
    def before_stopping(self, **kwargs):
        # - Stop all monitoring threads for tasks that were skipped, deferred, removed, etc.
        for task_instance in self.monitor_threads:
            if task_instance.state in STOPPED_TASK_INSTANCES_STATUSES:
                self._finalize_task(task_instance)


class CustomMetricsPlugin(AirflowPlugin):
    name = "CUSTOM_METRICS_PLUGIN"
    listeners = [TaskMonitorListener()]
