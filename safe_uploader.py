#!/usr/bin/env python
import argparse
import multiprocessing
import os
import psutil
import subprocess
import sys
import tempfile
import time
import urllib

from lib import environment_specific

ATTEMPTS = 5
BLOCK = 262144
S3_SCRIPT = '/usr/local/bin/gof3r'
SLEEP_TIME = .25
TERM_DIR = 'repeater_lock_dir'
TERM_STRING = 'TIME_TO_DIE'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("terminate_path",
                        help='When a file appears at this path, exit')
    args = parser.parse_args()

    while True:
        data = sys.stdin.read(BLOCK)
        if len(data) == 0:
            time.sleep(.25)

            # write empty data to detect broken pipes
            sys.stdout.write(data)

            if os.path.exists(args.terminate_path):
                if check_term_file(args.terminate_path):
                    sys.exit(0)
        else:
            sys.stdout.write(data)


def get_exec_path():
    """ Get the path to this executable

    Returns:
    the path as a string of this script
    """
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        __file__)
    if path.endswith('.pyc'):
        return path[:-1]
    else:
        return path


def get_term_dir():
    """ Get the directory where we will place files to communicate, creating
        it if needed.

    Returns
    a directory
    """
    term_dir = os.path.join(environment_specific.RAID_MOUNT, TERM_DIR)
    if not os.path.exists(term_dir):
        os.mkdir(term_dir)
    return term_dir


def get_term_file():
    """ Get a path to a file in the TERM_DIR which can be used to communicate

    Returns
    a path to a file created by tempfile.mkstemp
    """
    term_dir = get_term_dir()
    (handle, path) = tempfile.mkstemp(dir=term_dir)
    os.close(handle)
    return path


def check_term_file(term_path):
    """ Check to see if a term file has been populated with a magic string
        meaning that the repeater code can terminate

    Returns
    True if the file has been populated, false otherwise
    """
    with open(term_path, 'r') as term_handle:
        contents = term_handle.read(len(TERM_STRING))
    if contents == TERM_STRING:
        return True
    else:
        return False


def safe_upload(precursor_procs, stdin, bucket, key,
                check_func=None, check_arg=None):
    """ For sures, safely upload a file to s3

    Args:
    precursor_procs - A dict of procs that will be monitored
    stdin - The stdout from the last proc in precursor_procs that will be
             uploaded
    bucket - The s3 bucket where we should upload the data
    key - The name of the key which will be the destination of the data
    check_func - An optional function that if supplied will be run after all
                 procs in precursor_procs have finished.
    check_args - The arguments to supply to the check_func
    """
    devnull = open(os.devnull, 'w')
    try:
        term_path = get_term_file()
        repeater = subprocess.Popen([get_exec_path(), term_path],
                                    stdin=stdin,
                                    stdout=subprocess.PIPE)
        uploader = subprocess.Popen([S3_SCRIPT, 'put',
                                     '-k', urllib.quote_plus(key),
                                     '-b', bucket],
                                    stdin=repeater.stdout,
                                    stderr=devnull)
        success = False
        while not success:
            success = True
            for proc in precursor_procs:
                ret = precursor_procs[proc].poll()
                if ret is None:
                    # process has not yet terminated
                    success = False
                elif ret != 0:
                    raise Exception('{proc_id}: {proc} encountered an error'
                                    ''.format(proc_id=multiprocessing.current_process().name,
                                              proc=proc))

            # if we have success up to here, *and the term path does not exist* we
            # should run the check function and create the term_path.
            if success:
                if not check_term_file(term_path):
                    if check_func:
                        check_func(check_arg)
                    with open(term_path, 'w') as term_handle:
                        term_handle.write(TERM_STRING)

            # After checking the precursor_procs, next check the repeater
            ret = repeater.poll()
            if ret is None:
                success = False
            elif ret != 0:
                raise Exception('{proc_id}: repeater encountered an error'
                                ''.format(proc_id=multiprocessing.current_process().name))

            # Finally, check the uploader.
            ret = uploader.poll()
            if ret is None:
                success = False
            elif ret != 0:
                raise Exception('{proc_id}: uploader encountered an error'
                                ''.format(proc_id=multiprocessing.current_process().name))

            if not success:
                time.sleep(SLEEP_TIME)
    except:
        if uploader and psutil.pid_exists(uploader.pid):
            try:
                uploader.kill()
            except:
                pass
        if repeater and psutil.pid_exists(repeater.pid):
            try:
                repeater.kill()
            except:
                pass
        raise
    finally:
        os.remove(term_path)


def kill_precursor_procs(procs):
    """ In the case of a failure, we will need to kill off the precursor_procs

    Args:
    procs - a set of processes
    """
    for proc in procs:
        if procs[proc] and psutil.pid_exists(procs[proc].pid):
            try:
                procs[proc].kill()
            except:
                # process no longer exists, no big deal.
                pass


if __name__ == "__main__":
    main()
