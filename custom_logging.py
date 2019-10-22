"""

"""
import datetime
import logging
import time


def log(msg, file_name):
    """
    logs the given msg to the specified log file
    :param msg: string
    :param file_name: string
    :return: None
    """
    logging.basicConfig(filename=file_name + ".log", level=logging.INFO)
    time_stamp = time.time()
    content = str(msg) + " "
    content += datetime.datetime.fromtimestamp(time_stamp).strftime("%Y-%m-%d %H:%M:%S")
    logging.info(content)
