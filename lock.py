import os
import sys
import datetime
import logging

def create_lock_file(path):
    """
    Try and create a lock file atomically
    returns true if file @ path was successfuly created
    returns false if file already existed
    """
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        pid = str(os.getpid()).encode("utf-8")
        os.write(fd, pid)
        os.close(fd)
        return True
    except OSError:
        return False


def release_lock_file(path):
    """
    Remove lock file. Unsafe as owner of lock is assumed to be 
    the one who calls it. Footguns are
    * non lock owner can remove lock owned by some other proc
    * if lock doesn't exist, error is raised
    """
    os.remove(path)
    