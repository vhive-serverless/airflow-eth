# Remote XComm (ver. 0.2.0)
`Remote XComm` is a gRPC-based communication method for connecting between airflow's scheduler and workers.
You can verify the implementation of the `H2C Connector` in more detail in `./docs`.


## How to use it (Examples)
```
# test/server.py
import remote_xcomm as rx

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=8081)
    args = parser.parse_args()

    ti = DummyTaskInstance()
    rx.connect_ti_function(port=args.port, ti=ti, func="_run_raw_task") # HERE!
```
- Easily connect your `task instance` method function to `gRPC`!
   - Before connecting `task instance` to gRPC, define and generate `task instance` at first.
   - And then, connect `task instance` to gRPC, using `hc.connect_ti_function(port=..., ti=..., func=...)`
```
# test/client.py
import remote_xcomm as rx

if __name__ == "__main__":
    data = {...}
    response = rx.requests(url=..., port=80, data=data)
    print(response)
```
- Easily send a message (`dict`/`json`-type) to `task instance` via `hc.ConnectorClient(...)`.
   - **Note** You need to know the url of `task instance`.

## Advanced examples for `TaskInstance`
If you want to apply it in `_run_raw_task()` in `TaskIntance`,
```
    # airflow/models/taskinstance.py
    @provide_session
    @Sentry.enrich_errors
    def _run_raw_task(
        self,
        mark_success: bool = False,
        test_mode: bool = False,
        job_id: str | None = None,
        pool: str | None = None,
        session: Session = NEW_SESSION,
    ) -> TaskReturnCode | None:
        ...
        paylaods = self.rxcomm.get() # Add this code before executing the task!

        # Update `context`.
        # Run `self._execute_task_with_callbacks(context, test_mode)`.

        self.rxcomm.push(data)  # Add this code if the task is complete!
        ...
```
```
    # airflow/cli/commands/task_command.py
    import remote_xcomm as rx

    ...

    @cli_utils.action_cli(check_db=False)
    def task_run(args, dag: DAG | None = None) -> TaskReturnCode | None:
        ...
        task = _dag.get_task(task_id=args.task_id)
        ti, _ = _get_ti(task, args.map_index, exec_date_or_run_id=args.execution_date_or_run_id, pool=args.pool)

        rx.connect_ti_function(port=..., ti=ti, method='_run_raw_task') # Add this code in here!

        # Ignore below codes
        ti.init_run_context(raw=args.raw)
```

## TODO
- [ ] Find a better solution for attaching `Connector` to `task instance` used in airflow.
- [ ] Attach `H2C Connector` to airflow and test it.
