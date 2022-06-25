import os
import logging

logger = logging.getLogger()

dir_path = os.path.dirname(os.path.realpath(__file__))
file_handler = logging.FileHandler(f'{dir_path}/../log/{os.path.basename(__file__)}.log')
stream_handler = logging.StreamHandler()

formatter = logging.Formatter(
        '%(asctime)s %(levelname)-8s %(message)s')
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# logger.addHandler(stream_handler)
logger.addHandler(file_handler)

logger.setLevel(logging.INFO) # default, can be overried by other files
