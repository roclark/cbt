import argparse
import os
import subprocess
import sys

from bandwidth_class import Bandwidth
from fio_class import FioClass
from latency_class import Latency


CONFIDENCE_THRESHOLD = '50'
MAX_PERCENT_DEV = '100'
NO_REGRESSION = 0
REGRESSION = 10
REJECTED = 11
NOT_ENOUGH_SAMPLES = 12

REGRESSION_ARRAY = [NO_REGRESSION, REGRESSION, REJECTED, NOT_ENOUGH_SAMPLES]

IS_REGRESSION_FILE = '%s/is-regression.py'\
    % (os.path.dirname(os.path.abspath(__file__)))
STANDARD_FILES_DIRECTORY = '%s/../regression_standards/'\
    % (os.path.dirname(os.path.abspath(__file__)))

ERROR_INVALID_DIRECTORY = 1

EXIT_SUCCESS = 0


def process_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('data_directory', help='Parent directory where\
                        parsed data is saved (eg. ~/cbt_data/)', type=str)
    parser.add_argument('-c', '--confidence', help='Specify as a percentage how\
                        likely the baseline and sample data is different, \
                        defaults to %s' % (CONFIDENCE_THRESHOLD), type=str,
                        nargs='?', default=CONFIDENCE_THRESHOLD)
    parser.add_argument('-m', '--max_percent_dev', help='Specify as a\
                        a percentage the maximum allowed percentage deviant \
                        between baseline and sample data, defaults to %s'
                        % (MAX_PERCENT_DEV), type=str, nargs='?',
                        default=MAX_PERCENT_DEV)
    parser.add_argument('-v', '--verbose', help='Display more output results',
                        action='store_true')
    return parser.parse_args()

def standard_files():
    standard_list = []

    for root, dirs, files in os.walk(STANDARD_FILES_DIRECTORY):
        for filename in files:
            standard_list.append(os.path.join(root, filename))
    return standard_list

def get_files(directory):
    print 'Running regression tests. This could take a few minutes depending on volume...'
    file_list = []

    for root, dirs, files in os.walk(directory):
        for name in files:
            name = os.path.join(root, name)
            if os.stat(name).st_size != 0:
                file_list.append(name)
    return file_list

def exit_failure(message, code):
    print message
    sys.exit(code)

def save_data(data, filename, note):
    out_file = open(filename, 'w')
    out_file.write(note)
    for value in data:
        try:
            float_val = float(value)
            out_file.write('%f\n' % float_val)
        except ValueError:
            pass
    out_file.close()

def output_status(current, total):
    percent = 100 * current / total
    num_bars = percent / 2
    sys.stdout.write('\r%s%% [' % (str(percent).rjust(3)))
    for i in range(0, 50):
        if i < num_bars:
            sys.stdout.write('=')
        else:
            sys.stdout.write(' ')
    sys.stdout.write(']')
    sys.stdout.flush()

def perform_regression(sample_type, confidence, max_dev, standard, filename,
                       regression_list, unknown_status, empty_list):
    f = open(filename, 'r').readlines()
    if len(f) < 5:  # If the data is smaller than the header, skip
        empty_list.append(filename)
        return
    result = os.system('python %s %s %s %s %s %s > /dev/null'
                       % (IS_REGRESSION_FILE, sample_type, confidence, max_dev,
                          standard, filename))
    result /= 256
    if result in REGRESSION_ARRAY:
        regression_list.append([filename, result])
    else:
        unknown_status.append([filename, result])

def iterate_files(files, standards, verbose, confidence, max_percent_dev):
    current = 0
    empty_list = []
    regression_list = []
    total = len(files)
    unknown_status = []

    if len(files) == 0:
        exit_failure('Error, no output files found', ERROR_INVALID_DIRECTORY)
    for filename in files:
        current += 1
        output_status(current, total)
        for standard in standards:
            if standard.split('/')[-1] in filename:
                sample_type = ''
                if 'latency' in filename.lower():
                    sample_type = 'response-time'
                elif 'bandwidth' in filename.lower() or\
                     'iops' in filename.lower():
                    sample_type = 'throughput'
                perform_regression(sample_type, confidence,
                                   max_percent_dev, standard, filename,
                                   regression_list, unknown_status, empty_list)
    sys.stdout.write('\n')
    output_results(empty_list, regression_list, unknown_status, verbose)

def verbose_results(output_list, message):
    print '\n%s' % (message)
    for test in output_list:
        print '  %s' % (test.split('/')[-1])

def verbose_unknown_output(output_list, message):
    print '\n%s' % (message)
    for test in output_list:
        print '  %s: %s' % (test[0].split('/')[-1], test[1])

def output_results(empty_list, regression_list, unknown_status, verbose):
    num_incomplete = 0
    num_regression = 0
    incomplete = []
    regression = []
    total = len(regression_list)

    for test in regression_list:
        if test[1] == REGRESSION:
            num_regression += 1
            if verbose:
                regression.append(test[0])
        elif test[1] == REJECTED:
            num_incomplete += 1
            if verbose:
                incomplete.append(test[0])

    if num_regression > 0:
        print '\nRegression found in %s of %s tests' % (num_regression, total)
        if verbose:
            verbose_results(regression, 'The following tests have regression:')
    else:
        print '\nNo regression found'

    if num_incomplete > 0 and verbose:
        verbose_results(incomplete, 'The following tests were rejected due to'
                                    ' large percentage deviation beyond input\n'
                                    'confidence and max percentage deviation '
                                    'thresholds:')

    if len(empty_list) > 0 and verbose:
        verbose_results(empty_list, 'The following tests were skipped'
                                    ' - No results found:')

    if len(unknown_status) > 0 and verbose:
        verbose_unknown_output(unknown_status, 'The following tests retrieved'
                                               ' an unknown regression status:')

if __name__ == "__main__":
    args = process_arguments()
    files = get_files(args.data_directory)
    standards = standard_files()
    iterate_files(files, standards, args.verbose, args.confidence,
                  args.max_percent_dev)
    sys.exit(EXIT_SUCCESS)
