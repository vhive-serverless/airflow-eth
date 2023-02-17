#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import re
import sys
import cProfile
from airflow.__main__ import main
if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw|\.exe)?$', '', sys.argv[0])
    cProfile.run('main()', sort="cumulative")
    sys.exit()
