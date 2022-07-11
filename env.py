import os
import tempfile
import random
import glob
import requests
import sys
import copy
import json
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

from utils.logger import logger

ROOT_PATH = os.getcwd()
logger.debug('You are not alone.')
logger.info(f'Enviroment loaded. Working Dir: {ROOT_PATH}')
