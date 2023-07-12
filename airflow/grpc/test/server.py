import argparse
import time
import uuid
import logging

import remote_xcomm as rx


class DummyTaskInstance:
    def _run_raw_task(
        self,
        mark_success=None,
        test_mode=None,
        job_id=None,
        pool=None,
        session=None,
    ):
        req_payloads = self.rxcomm.get()

        logging.info(req_payloads)
        time.sleep(0.5)
        resp_payloads = {
            "output": str(uuid.uuid4()),
            "output_key": "hello_world"
        }

        self.rxcomm.push( resp_payloads )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=8081)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    rx.use_rxcomm_logging()

    logging.info(f"Starting...")
    args = main()
    logging.info(f"Got arguments...")

    ti = DummyTaskInstance()
    rx.connect_ti_function(ti=ti, method="_run_raw_task", port=args.port)
