import os
import sys
import datetime
import logging
def create_lock_file(path):
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except OSError:
        return False


def release_lock_file(path):
    os.remove(path)
    