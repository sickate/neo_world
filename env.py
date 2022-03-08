import os
import tempfile
import random
import glob
import requests
import sys
sys.path.append('./')

import numpy as np

import missingno as msno
from tqdm.auto import trange, tqdm

import pendulum as pdl
from time import sleep
from timeit import timeit
from datetime import datetime, timedelta

import talib as ta
import mplfinance as mpf

import modin.pandas as ppd
import pandas as pd

from IPython.core.display import display

ROOT_PATH = os.getcwd()
print(f'[{pdl.now()}] Enviroment loaded. Working Dir: {ROOT_PATH}')
