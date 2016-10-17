# Compression lookback dictionary creation tools

## Tools
  - **zdict_freqs.py**
This tool will accepts two files as arguments. These files should be populated
with example data, with each line having a separate instance of data to be
compared. The script will read through each files and compare the data on the
first line of the first file to the first line of the second file, the second
line of the first file to the second line of the second and so on.

When the script is finished, it will output a JSON encoded dictionary of
common substrings.

  - **zdict_gen.py**
This tool will accept a file with the output from zdict_freqs.py and an
optional --size argument. The script will then construct a lookback compression
dictionary limited to a size defined by the --size argument.

