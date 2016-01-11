#!/usr/bin/env python

import argparse
import os
import sys

CEPH_CONFIG_DEFAULT = 'ceph.conf'
YAML_DEFAULT = 'cbt_config.xfs.yaml'

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--all', help='Run all benchmarks, parse the data\
        and run regression tests', action='store_true')
    parser.add_argument('-b', '--benchmark', help='Run all benchmarks specified\
        in the CBT config file', action='store_true')
    parser.add_argument('-c', '--ceph_config', help='Specify ceph config file,\
        defaults to \'%s\'' % (CEPH_CONFIG_DEFAULT), type=str, nargs='?',
                        default=CEPH_CONFIG_DEFAULT)
    parser.add_argument('-o', '--output_dir', help='Where applicable, specify\
        the directory to output data files (e.g. ~/cbt_data/)', type=str,
                        nargs='?', default='')
    parser.add_argument('-p', '--parser', help='Run the data parser,\
        applicable if benchmarks have already been run', action='store_true')
    parser.add_argument('-r', '--regression', help='Run the automated\
        regression tests, applicable if data has been parsed',
                        action='store_true')
    parser.add_argument('-v', '--verbose', help='Where applicable, display more\
        output results', action='store_true')
    parser.add_argument('-y', '--yaml_file', help='Specify YAML config file,\
        defaults to \'%s\'' % (YAML_DEFAULT), type=str, nargs='?',
                        default=YAML_DEFAULT)
    parser.add_argument('data_directory', help='Where applicable, specify\
        parent directory where benchmark results are saved (e.g. ~/benchmark_\
        results/)', type=str)
    return parser.parse_args()

def run_benchmarks(cwd, data_dir, yaml_file, ceph_config):
    if yaml_file == YAML_DEFAULT:
        yaml_file = '%s/%s' % (cwd, yaml_file)
    if os.path.isfile(yaml_file) is False:
        print 'Config file not found. To create config file, run \'python '\
            '%s/config_wizard.py\'' % (cwd)
        sys.exit(1)
    if ceph_config is CEPH_CONFIG_DEFAULT:
        ceph_config = ''
    else:
        ceph_config = '-s %s' % (ceph_config)
    os.system('%s/../cbt.py --archive=%s %s %s'
              % (cwd, data_dir, yaml_file, ceph_config))

def run_regression(cwd, data_dir, verbose):
    if verbose:
        verbose = '-v'
    else:
        verbose = ''
    command = 'python %s/regression_automation.py %s %s'\
        % (cwd, data_dir, verbose)
    os.system(command)

def parse_data(cwd, out_dir, data_dir):
    if out_dir is '':
        out_dir = os.getcwd()
    command = 'python %s/parse_data.py -o %s %s' % (cwd, out_dir, data_dir)
    os.system(command)

def determine_tests(args, cwd):
    if args.all:
        run_benchmarks(cwd, args.data_directory, args.yaml_file,
                       args.ceph_config)
        parse_data(cwd, args.output_dir, args.data_directory)
        run_regression(cwd, args.data_directory, args.verbose)
    if args.benchmark and not args.all:
        run_benchmarks(cwd, args.data_directory, args.yaml_file,
                       args.ceph_config)
    if args.parser and not args.all:
        parse_data(cwd, args.output_dir, args.data_directory)
    if args.regression and not args.all:
        if args.benchmark and not args.parser:
            print 'Error: Must parse data (-p) before running regression tests'
            sys.exit(1)
        if args.parser:
            run_regression(cwd, args.output_dir, args.verbose)
        else:
            run_regression(cwd, args.data_directory, args.verbose)
    if not args.all and not args.benchmark and not args.parser\
       and not args.regression:
        run_benchmarks(cwd, args.data_directory, args.yaml_file,
                       args.ceph_config)

def main():
    cwd = os.path.dirname(os.path.abspath(__file__))
    args = parse_arguments()
    determine_tests(args, cwd)

if __name__ == "__main__":
    main()
