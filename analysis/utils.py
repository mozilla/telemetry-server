from errno import EEXIST
from multiprocessing import active_children, current_process
import os

def mkdirp(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != EEXIST or not os.path.isdir(path):
            raise


