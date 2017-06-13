import logging
import multiprocessing
import os
import subprocess
import tempfile
import time
import urllib

import boto
import psutil

PROGRESS_PROC = 'progress'
PV = ['/usr/bin/pv', '-peafbt']
PYTHON = 'python'
REPEATER_PROC = 'repeater'
REPEATER_SCRIPT = 'safe_uploader_repeater.py'
S3_SCRIPT = '/usr/local/bin/gof3r'
SLEEP_TIME = .25
TERM_STRING = 'TIME_TO_DIE'
UPLOADER_PROC = 'uploader'

log = logging.getLogger(__name__)


def safe_upload(precursor_procs, stdin, bucket, key,
                check_func=None, check_arg=None, verbose=False):
    """ For sures, safely upload a file to s3

    Args:
    precursor_procs - A dict of procs that will be monitored
    stdin - The stdout from the last proc in precursor_procs that will be
            uploaded
    bucket - The s3 bucket where we should upload the data
    key - The name of the key which will be the destination of the data
    check_func - An optional function that if supplied will be run after all
                 procs in precursor_procs have finished. If the uploader should
                 abort, then an exception should be thrown in the function.
    check_args - The arguments to supply to the check_func
    verbose - If True, display upload speeds statistics and destination
    """
    upload_procs = dict()
    devnull = open(os.devnull, 'w')
    term_path = None
    try:
        term_path = get_term_file()
        if verbose:
            log.info('Uploading to s3://{buk}/{key}'.format(buk=bucket,
                                                            key=key))
            upload_procs[PROGRESS_PROC] = subprocess.Popen(
                PV,
                stdin=stdin,
                stdout=subprocess.PIPE)
            stdin = upload_procs[PROGRESS_PROC].stdout
        upload_procs[REPEATER_PROC] = subprocess.Popen(
            [PYTHON, get_exec_path(), term_path],
            stdin=stdin,
            stdout=subprocess.PIPE)
        upload_procs[UPLOADER_PROC] = subprocess.Popen(
            [S3_SCRIPT,
             'put',
             '-k', urllib.quote_plus(key),
             '-b', bucket],
            stdin=upload_procs[REPEATER_PROC].stdout,
            stderr=devnull)

        # While the precursor procs are running, we need to make sure
        # none of them have errors and also check that the upload procs
        # also don't have errors.
        while not check_dict_of_procs(precursor_procs):
            check_dict_of_procs(upload_procs)
            time.sleep(SLEEP_TIME)

        # Once the precursor procs have exited successfully, we will run
        # any defined check function
        if check_func:
            check_func(check_arg)

        # And then create the term file which will cause the repeater and
        # uploader to exit
        write_term_file(term_path)

        # And finally we will wait for the uploader procs to exit without error
        while not check_dict_of_procs(upload_procs):
            time.sleep(SLEEP_TIME)
    except:
        clean_up_procs(upload_procs, precursor_procs)
        raise
    finally:
        if term_path:
            os.remove(term_path)

    # Assuming all went well, return the new S3 key.
    conn = boto.connect_s3()
    bucket_conn = conn.get_bucket(bucket, validate=False)
    return bucket_conn.get_key(key)


def get_exec_path():
    """ Get the path to this executable

    Returns:
    the path as a string of this script
    """
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        REPEATER_SCRIPT)
    return path


def get_term_file():
    """ Get a path to a file which can be used to communicate

    Returns
    a path to a file created by tempfile.mkstemp
    """
    (handle, path) = tempfile.mkstemp()
    os.close(handle)
    return path


def write_term_file(term_path):
    """ Create the termination file

    Args:
    term_path - Where to write the magic string to terminate the repeater
    """
    with open(term_path, 'w') as term_handle:
        term_handle.write(TERM_STRING)

def try_kill(proc):
    """ Try to kill a process

    Args:
    proc - A process created by subprocess.Popen
    """
    if not psutil.pid_exists(proc.pid):
        return

    try:
        proc.kill()
        proc.wait()
    except:
        pass


def check_dict_of_procs(proc_dict):
    """ Check a dict of process for exit, error, etc...

    Args:
    A dict of processes

    Returns: True if all processes have completed with return status 0
             False is some processes are still running
             An exception is generated if any processes have completed with a
             returns status other than 0
    """
    success = True
    for proc in proc_dict:
        ret = proc_dict[proc].poll()
        if ret is None:
            # process has not yet terminated
            success = False
        elif ret != 0:
            if multiprocessing.current_process().name != 'MainProcess':
                proc_id = '{}: '.format(multiprocessing.current_process().name)
            else:
                proc_id = ''

            raise Exception('{proc_id}{proc} encountered an error'
                            ''.format(proc_id=proc_id,
                                      proc=proc))
    return success


def clean_up_procs(upload_procs, precursor_procs):
    """ Clean up the pipeline procs in a safe order

    Args:
    upload_procs -  A dictionary of procs used for the upload
    precursor_procs - A dictionary of procs that feed the uploader
    """
    # So there has been some sort of a problem. We want to make sure that
    # we kill the uploader so that under no circumstances the upload is
    # successfull with bad data
    if UPLOADER_PROC in upload_procs:
        try_kill(upload_procs[UPLOADER_PROC])
        del upload_procs[UPLOADER_PROC]

    # Next the repeater and the pv (if applicable)
    for proc in upload_procs:
        try_kill(upload_procs[proc])

    # And finally whatever is feeding the uploader
    for proc in precursor_procs:
        try_kill(precursor_procs[proc])
