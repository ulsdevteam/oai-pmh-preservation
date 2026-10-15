import os
import sys
import datetime

def create_lock_file(path):
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except OSError:
        log.err("Lock File found, another program is currently modifying the directories...")
        return False
        

def release_lock_file(path):
    os.remove(path)
    