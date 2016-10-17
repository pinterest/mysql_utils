#!/usr/bin/python3.3

from __future__ import print_function

import argparse
import json
import multiprocessing
import os
import sys

from collections import OrderedDict
from hashlib import md5
from itertools import islice

'''
Zlib's implementation uses 262 bytes of overhead for pre-defined dictionary
thus, the max zdict size is 32KB - 262B = 32506B
'''
MAX_WINDOW_SIZE = 32506  # 32KB-262B, size of MAX_WBITS - zdict overhead
MAX_LOOKAHEAD_BUFFER_SIZE = 258  # Maximum Match Length in zlib
BATCHING_FACTOR = 3  # Number of times of cores to use as batching size
DEFAULT_ZDICT_SIZE = 32506  # Length of predefined dictionary to generate


def printProgress(iteration, total):
    """
    Call in a loop to show terminal progress

    Args:
        iteration (int): current iteration
        total     (int): total iterations

    Returns:
        None
    """
    fraction = min(iteration / float(total), 1.0)
    percent = round(100.00 * fraction, 2)
    sys.stderr.write('\r%s%s' % (percent, '%')),
    sys.stderr.flush()
    if fraction == 1.0:
        sys.stderr.write('\n')
        sys.stderr.flush()


def gotoLine(f, n):
    """
    Sets the file object to start at nth line (0 indexed)

    Args:
        f_obj (file): file object
        n     (int): nth line to jump to

    Returns:
        None
    """
    f.seek(0)
    if n > 0:
        for _ in islice(f, n):
            pass


def updatePinZDict(pin1, pin2):
    """
    Finds common substrings between both pins represenations

    Args:
        pin1 (str): data to find common strings against pin2
        pin2 (str): data to find common strings against pin1

    Returns:
        set(str): Set of substrings that are used for lookback in LZ77
                  among both pins bidirectionally
    """
    pin1_dict_set = getSubstringSet(pin1, pin2)
    pin2_dict_set = getSubstringSet(pin2, pin1)
    pin_dict_set = pin1_dict_set.intersection(pin2_dict_set)
    return pin_dict_set


def getSubstringSet(input_data,
                    zdict,
                    window_size=MAX_WINDOW_SIZE,
                    buffer_size=MAX_LOOKAHEAD_BUFFER_SIZE):
    """
    Finds longest matches for input_data using zdict as a predefined
    dictionary (pre-loaded window)

    Args:
        input_data  (str):
        zdict       (str):
        window_size (Optional[int]): lookback window size
        buffer_size (Optional[int]): lookahead buffer size

    Returns:
        set(str): Set of substrings that are used for lookback in LZ77
    """
    data = zdict  # pre-load window
    pos = len(zdict)  # starting position after pre-defined window
    data += input_data
    zDict = set()

    while pos < len(data):
        match = findLongestMatch(data, pos, window_size, buffer_size)
        if match:
            (bestMatchLength, bestMatchStr) = match
            zDict.add(bestMatchStr)
            pos += bestMatchLength
        else:
            pos += 1
    return zDict


def findLongestMatch(data,
                     current_position,
                     window_size=MAX_WINDOW_SIZE,
                     buffer_size=MAX_LOOKAHEAD_BUFFER_SIZE):
    """
    Finds the longest match to a substring starting at the current_position
    in the lookahead buffer from the history window

    Args:
        data             (str): data to find next longest match
        current_position (int): current position (index) in data
        window_size      (Optional[int]): lookback window size
        buffer_size      (Optional[int]): lookahead buffer size

    Returns:
        tuple(int, str): Tuple of length of best match found and the string
                         matched, None if no match found in lookahead buffer
    """
    end_of_buffer = min(current_position + buffer_size, len(data) + 1)

    best_match_distance = -1
    best_match_length = -1
    best_match_str = None

    # Optimization: Only consider substrings of length 3 and greater
    for j in range(current_position + 3, end_of_buffer):

        start_index = max(0, current_position - window_size)
        substring = data[current_position:j]

        for i in range(start_index, current_position):

            repetitions = len(substring) // (current_position - i)

            last = len(substring) % (current_position - i)

            matched_string = (data[i:current_position] * repetitions +
                              data[i:i+last])

            if matched_string == substring and \
               len(substring) > best_match_length:
                best_match_distance = current_position - i
                best_match_length = len(substring)
                best_match_str = substring

    if best_match_distance > 0 and best_match_length > 0:
        return (best_match_length, best_match_str)
    return None


def executeBatchFreqs(nlines_per_iter,
                      pins_file_1,
                      pins_file_2,
                      cores=multiprocessing.cpu_count()):
    # Grab next n lines from both files
    nlines1 = islice(pins_file_1, nlines_per_iter)
    nlines2 = islice(pins_file_2, nlines_per_iter)

    # Start processing n lines over args.cores processors
    pool = multiprocessing.Pool(processes=cores)
    list_sets = pool.starmap(updatePinZDict,
                             zip(nlines1,
                                 nlines2))

    return list_sets


def getMD5(fbase1, fbase2):
    md5_obj = md5()
    md5_obj.update(fbase1.encode())
    md5_obj.update(fbase2.encode())
    return md5_obj.hexdigest()


def saveState(fbase1, fbase2, state_fname, line_num, freq):
    """
    Saves state in state file

    State file is a 3 line file as follows

    Line    Contents
    ----    --------
    1       MD5 hash of first data file and second data file
    2       Last datafile line read
    3       Frequencies of substrings as json

    Args:
        fbase1      (str): base filename of first data file
        fbase2      (str): base filename of second data file
        state_fname (str): filename of state file
        line_num    (int): most recent line numebr read from
                           data files
        freqs       (dict of str:int): mapping of substring to
                                       occurring frequency
    Returns:
        None
    """

    md5_hash = getMD5(fbase1, fbase2)
    freq_json = json.dumps(freq)

    with open(state_fname, 'w+') as state_file:
        state_file_contents = '{0}\n{1}\n{2}'.format(md5_hash,
                                                     line_num,
                                                     freq_json)
        state_file.write(state_file_contents)


def restoreState(fbase1, fbase2, state_fname):
    """
    Attempts to read in previous state from state file

    State file should be a 3 line file

    Line    Contents
    ----    --------
    1       MD5 hash of first data file and second data file
    2       Last datafile line read
    3       Frequencies of substrings as json

    Args:
        fbase1      (str): base filename of first data file
        fbase2      (str): base filename of second data file
        state_fname (str): filename of state file

    Returns:
        (int, dict of str: int): line number of last datafile line read,
                                 frequencies of substrings (substr: freq)
                                 if able to parse file else None
    """
    md5_hash = getMD5(fbase1, fbase2)
    try:
        with open(state_fname, 'r+') as state_file:
            try:
                state_data = state_file.read()
                file_md5, line_num, freq_json = state_data.splitlines()
            except ValueError:
                raise Exception('Invalid statefile format')

            # Verify correct state file
            if file_md5 != str(md5_hash):
                raise Exception('State file does not correspond to input data')

            # Populate left off line number
            try:
                line_num = int(line_num)
            except ValueError:
                raise Exception('Invalid state line number')

            # Initialize frquencies dictionary
            try:
                freqs = json.loads(freq_json)
            except ValueError:
                raise Exception('Invalid substring frequency JSON')

    except IOError:
        print("State file doesn't exist", file=sys.stderr)
        raise
    return line_num, freqs


def parse():
    """
    Defines a cli and parses command line inputs

    Args:

    Returns:
        object(options): The returned object from an
                         argparse.ArgumentParser().parse_args() call
    """
    parser = argparse.ArgumentParser(
                        description="Takes in two files and looks for common "
                                    "substrings within the lookback window "
                                    "and lookahead buffer pairwise by line.")
    parser.add_argument('data1',
                        action='store',
                        help='First data file to generate LZ77 dict')
    parser.add_argument('data2',
                        action='store',
                        help='Second data file to generate LZ77 dict')
    parser.add_argument('--cores',
                        action='store',
                        type=int,
                        default=multiprocessing.cpu_count(),
                        help='Number of cores to utilize. '
                             'Default all cores.')
    parser.add_argument('--state',
                        action='store',
                        help='File to track progress for failure')
    return parser.parse_args()


def main():
    args = parse()

    line_num = 0  # current line number in file
    freqs = dict()  # common substring: frequency

    if args.state:
        _, data1_base_fname = os.path.split(args.data1)
        _, data2_base_fname = os.path.split(args.data2)

        try:
            line_num, freqs = restoreState(data1_base_fname,
                                           data2_base_fname,
                                           args.state)
        except OSError:
            print('Invalid state file. Starting anew.', file=sys.stderr)

    with open(args.data1, 'r') as pins_file_1, \
            open(args.data2, 'r') as pins_file_2:

        # Read only min number lines of file pair
        line_count_1 = sum(1 for _ in pins_file_1)
        line_count_2 = sum(1 for _ in pins_file_2)
        nlines = min(line_count_1, line_count_2)

        # Start at left off state. Default beginning of file
        gotoLine(pins_file_1, line_num)
        gotoLine(pins_file_2, line_num)

        nlines_per_iter = args.cores * BATCHING_FACTOR  # lines per batch

        while line_num < nlines:
            printProgress(line_num, nlines)
            list_sets = executeBatchFreqs(nlines_per_iter,
                                          pins_file_1,
                                          pins_file_2,
                                          args.cores)

            # Aggregate substrings per batch
            for s in list_sets:
                s = list(s)
                for i in s:
                    freqs[i] = freqs.get(i, 0) + 1

            if args.state:
                saveState(data1_base_fname,
                          data2_base_fname,
                          args.state,
                          line_num,
                          freqs)

            line_num += nlines_per_iter
            line_num = min(line_num, nlines)  # batching may go over nlines

        printProgress(line_num, nlines)
        # Output frequencies as JSON
        sorted_freqs = OrderedDict(sorted(freqs.items(),
                                          key=lambda k: k[1],
                                          reverse=True))
        sorted_freqs_json = json.dumps(sorted_freqs,
                                       indent=4,
                                       separators=(',', ': '))
        print(sorted_freqs_json, end='')

        # cleanup state file upon completion
        if args.state:
            os.remove(args.state)


if __name__ == '__main__':
    main()
