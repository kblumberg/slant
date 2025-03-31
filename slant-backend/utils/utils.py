import re
import os
import pandas as pd
from constants.constant import DEBUG_MODE

def get_base_path():
    return re.split('utils', os.path.dirname(os.path.abspath(__file__)))[0]

def read_csv(path):
    return pd.read_csv(path)

def write_csv(df, path):
    df.to_csv(path, index=False)

def log(message):
    if DEBUG_MODE:
        print(message)
