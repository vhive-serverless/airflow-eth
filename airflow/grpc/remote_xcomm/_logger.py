from typing import *
import logging


class _BaseLogger:
    def __init__(self, name: str) -> None:
        self.logger = logging.getLogger( name=name )
        self.logger.setLevel(logging.INFO)

    def info(self, msg: str):
        return self.logger.info(msg)


def use_rxcomm_logging():
    logging.basicConfig(
        format='[%(asctime)s : %(name)s ] %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S',
        level=logging.INFO,
    )

