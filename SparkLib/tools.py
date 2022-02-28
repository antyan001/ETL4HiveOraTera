#!/home/ektov1-av_ca-sbrf-ru/bin/python35

################################## Import ##################################

import os
import sys
import warnings
import subprocess
import re
import abc
import argparse

_labdata = os.environ.get("LABDATA_PYSPARK")
sys.path.insert(0, _labdata)
os.chdir(_labdata)

warnings.filterwarnings('ignore')

curruser = os.environ.get('USER')

if curruser in os.listdir("/opt/workspace/"):
    sys.path.insert(0, '/opt/workspace/{user}/notebooks/support_library/'.format(user=curruser))
    sys.path.insert(0, '/opt/workspace/{user}/libs/python3.5/site-packages/'.format(user=curruser))
    sys.path.insert(0, '/opt/workspace/{user}/notebooks/labdata/lib/'.format(user=curruser))
else:
    sys.path.insert(0, '/home/{}/notebooks/support_library/'.format(curruser))
    sys.path.insert(0, '/home/{}/python35-libs/lib/python3.5/site-packages/'.format(curruser))
    sys.path.insert(0, '/home/{}/notebooks/labdata/lib/'.format(curruser))

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns
import json
from datetime import datetime
import inspect
import tendo.singleton
from collections import Counter
import time

from lib.logger import *

def exception_restart(num_of_attempts: int = 3,
                      delay_time_sec: int = 10):

    def decorator(func):

        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(num_of_attempts):
                try:
                    func_return = func(*args, **kwargs)
                    return func_return
                except Exception as exc:
                    time.sleep(delay_time_sec)
                    last_exception = exc
                    continue
            raise last_exception

        return wrapper

    return decorator


__PYSPARK_TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'pyspark_templates')
PYSPARK_HIVE_STATIC_KRBR_TGT = os.path.join(__PYSPARK_TEMPLATE_DIR, 'pyspark_hive_static_krbr_tgt')
