import os

APP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

CONFIG_DIR = os.path.realpath("{}/config".format(APP_DIR))
CONFIG_FILE_ARRAYS = "{}/config.json".format(CONFIG_DIR)
