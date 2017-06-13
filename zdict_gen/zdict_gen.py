#!/usr/bin/python3.3

from __future__ import print_function

import argparse
import json


'''
Zlib's implementation uses 262 bytes of overhead for pre-defined dictionary
thus, the max zdict size is 32KB - 262B = 23506B
'''
MAX_WINDOW_SIZE = 32506  # 32KB-262B, size of MAX_WBITS - zdict overhead
MAX_LOOKAHEAD_BUFFER_SIZE = 258  # Maximum Match Length in zlib
BATCHING_FACTOR = 3  # Number of times of cores to use as batching size
DEFAULT_ZDICT_SIZE = 32506  # Length of predefined dictionary to generate


'''
TODO

Implement a more optimal shortest common superstring solution.
This would allow more relevant substrings to fit within the pre-defined
dictionary window. In order to generalize it for compression against
larger data objects the solution would have to account for more
commonly occuring substrings to be placed towards the end of the
dictionary. As far as I know, the vanilla SCS problem is NP-complete,
not including the previously stated constraint.
'''


def genDictFromFreq(freq_dict, size_b):
    """
    Creates a LZ77 dictionary (initial lookback window) from a dictionary of
    word: frequency

    Args:
        freq_dict   (dict(str)): A dictionary mapping word to frequency
        size_b      (int): output size of frequency dictionary in B

    Returns:
        str: A LZ77 dictionary of size_b scored on len(word) * frequency
    """

    # change value from frequency to score
    for word, freq in freq_dict.items():
        freq_dict[word] = len(word) * freq

    """ superstrings swallow substring scores """
    # 1. sort keys on increasing key (word) length
    sorted_keys = sorted(freq_dict, key=lambda k: len(k))

    # 2. add substring scores to superstring value and flag for
    #    removal (set score to 0)
    for i, key_i in enumerate(sorted_keys):
        # highest scoring superstring should consume substring
        sorted_keys_by_score = sorted(sorted_keys[i+1:],
                                      key=lambda k: freq_dict[k],
                                      reverse=True)
        for j, key_j in enumerate(sorted_keys_by_score):
            if key_i in key_j:
                freq_dict[key_j] += freq_dict[key_i]
                freq_dict[key_i] = 0
                break

    # 3. Remove substring items (has score 0)
    freq_dict = {k: v for k, v in freq_dict.items() if v > 0}

    """ Create LZ77 dictionary string """
    # 1. Join keys (word) on score in ascending) order
    # According to zlib documentation, most common substrings should be placed
    # at the end of the pre-defined dictionary
    dict_str = ''.join(sorted(freq_dict,
                              key=lambda k: freq_dict[k]))

    # 2. trim to size_b if valid
    if 0 < size_b < len(dict_str):
        dict_str = dict_str[len(dict_str)-size_b:]

    return dict_str


def parse():
    """
    Defines a cli and parses command line inputs

    Args:

    Returns:
        object(options): The returned object from an
                         argparse.ArgumentParser().parse_args() call
    """
    parser = argparse.ArgumentParser(
                        description="Generates a *good* predefined dictionary "
                                    "for compressing pin objects with the "
                                    "DEFLATE algorithm. Takes in a file of "
                                    "commonly occuring substrings and their "
                                    "frequencies. "
                                    "Common substring occurances are scored "
                                    "from the product of length of substring "
                                    "and the frequency with which it occurs. "
                                    "Fully contained substrings are swallowed "
                                    "and the top scoring strings are "
                                    "concatenated up to SIZE bytes.")
    parser.add_argument('freqs_file',
                        action='store',
                        help="File of commonly occuring substrings. Reads in "
                             "json with substring keys and frequency values")
    parser.add_argument('--size',
                        action='store',
                        type=int,
                        default=DEFAULT_ZDICT_SIZE,
                        help='Size of predefined dictionary to generate')
    return parser.parse_args()


def main():
    args = parse()

    with open(args.freqs_file, 'r') as freqs_file:
        counts = json.load(freqs_file)

    # Generate Dictionary from substring frequencies
    zdict_str = genDictFromFreq(counts, args.size)
    print(zdict_str, end='')


if __name__ == '__main__':
    main()
