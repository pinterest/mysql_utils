#!/usr/bin/env python
""" This module will is a helper to safe_uploader and should not be called
    directly.
"""
import argparse
import os
import re
import subprocess
import sys
import time

import safe_uploader


BLOCK_SIZE = 262144
INIT_PID = 1
SLEEP_TIME = .25
STDIN = 0
STDOUT = 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("terminate_path",
                        help='When a file appears at this path, exit')
    args = parser.parse_args()
    repeater(args.terminate_path)


def repeater(terminate_path):
    """ Repeat data from stdin to stdout until no more data is present comes
    from stdin AND a stop file is populated with a magic value.

    Args:
    terminate_path - The path of the stop file
    """
    while True:
        if os.getppid() == INIT_PID:
            kill_stdout_reader()
            raise Exception('Safe uploader proc is now somehow the child of '
                            'proc 1. This means that that parent of the '
                            'repeater process no longer is no longer in '
                            'control. Lacking any good option, the repeater '
                            'process will terminate.')

        data = sys.stdin.read(BLOCK_SIZE)
        if len(data) == 0:
            time.sleep(SLEEP_TIME)

            # write empty data to detect broken pipes
            sys.stdout.write(data)

            if os.path.exists(terminate_path):
                if check_term_file(terminate_path):
                    sys.exit(0)
        else:
            sys.stdout.write(data)

        if len(data) < BLOCK_SIZE:
            sys.stdout.flush()


def check_term_file(term_path):
    """ Check to see if a term file has been populated with a magic string
        meaning that the repeater code can terminate

    Returns
    True if the file has been populated, false otherwise
    """
    with open(term_path, 'r') as term_handle:
        contents = term_handle.read(len(safe_uploader.TERM_STRING))
    return contents == safe_uploader.TERM_STRING


def kill_stdout_reader():
    """ Kill whatever is on the otherside of stdout """
    std_out_fd = '/proc/{pid}/fd/{stdout}'.format(pid=os.getpid(),
                                                  stdout=STDOUT)
    readlink = os.readlink(std_out_fd)
    pipe_node = re.match('pipe:\[([0-9]+)]', readlink).groups()[0]
    cmd = ("lsof | "
           "awk '{{if($4 == \"{stdin}r\" && $8 == {pipe_node}) print $2}}'"
           "".format(stdin=str(STDIN),
                     pipe_node=pipe_node))
    lsof = subprocess.Popen(cmd, shell=True,
                            stdout=subprocess.PIPE)
    lsof.wait()
    stdout_reader_pid = int(lsof.stdout.read())
    try:
        os.kill(stdout_reader_pid, 9)
    except:
        pass
        # Nothing really to be done here, it is probalby hopeless to try
        # to do anything more.


if __name__ == "__main__":
    main()
