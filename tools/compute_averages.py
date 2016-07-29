import argparse
import os
import re
import sys


ERROR_INVALID_DIRECTORY = 1

EXIT_SUCCESS = 0


def process_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('data_directory', help='Parent directory where\
        parsed data is saved (eg. ~/parsed_data/)', type=str)
    parser.add_argument('-o', '--overall_only', help='When specified, only the\
        overall results from the cluster for each test are displayed as opposed\
        to the results from every host', action='store_true')
    return parser.parse_args()

def get_files(directory):
    rados_file_list = []
    kvmrbd_file_list = []
    rbd_file_list = []

    for root, dirs, files in os.walk(directory):
        for name in files:
            if 'KvmRbdFio' in name:
                kvmrbd_file_list.append(os.path.join(directory, name))
            elif 'Radosbench' in name:
                rados_file_list.append(os.path.join(directory, name))
            elif 'rbdfio' in name:
                rbd_file_list.append(os.path.join(directory, name))
    return kvmrbd_file_list, rados_file_list, rbd_file_list

def calculate_average(filehandle):
    num = 0.0
    total = 0.0
    unit = ''

    for line in filehandle:
        if line.startswith('#'):
            find = re.findall(r'in .+ using', line)
            if len(find) > 0:
                unit = find[0].split(' ')[1]
        else:
            total += float(line)
            num += 1.0
    return total, num, unit

def rbd_data(rbd_files):
    randread_bandwidth = []
    randread_iops = []
    randread_latency = []
    randwrite_bandwidth = []
    randwrite_iops = []
    randwrite_latency = []
    read_bandwidth = []
    read_iops = []
    read_latency = []
    write_bandwidth = []
    write_iops = []
    write_latency = []
    hosts = []

    for filename in rbd_files:
        f = open(filename, 'r')
        total, num, unit = calculate_average(f)
        file_split = filename.split('/')[-1].split('-')
        test_type = filename.split('/')[-1].split('_')
        if len(file_split) > 1:
            hostname = '-'.join(file_split[1:])
            if hostname not in hosts:
                hosts.append(hostname)
        else:
            f.close()
            continue
        test = ' '.join(test_type[3:5]).split('-')[0]
        if test == 'randread bandwidth':
            randread_bandwidth.append([total, num, hostname, unit])
        elif test == 'randread iops':
            randread_iops.append([total, num, hostname, unit])
        elif test == 'randread latency':
            randread_latency.append([total, num, hostname, unit])
        elif test == 'randwrite bandwidth':
            randwrite_bandwidth.append([total, num, hostname, unit])
        elif test == 'randwrite iops':
            randwrite_iops.append([total, num, hostname, unit])
        elif test == 'randwrite latency':
            randwrite_latency.append([total, num, hostname, unit])
        elif test == 'read bandwidth':
            read_bandwidth.append([total, num, hostname, unit])
        elif test == 'read iops':
            read_iops.append([total, num, hostname, unit])
        elif test == 'read latency':
            read_latency.append([total, num, hostname, unit])
        elif test == 'write bandwidth':
            write_bandwidth.append([total, num, hostname, unit])
        elif test == 'write iops':
            write_iops.append([total, num, hostname, unit])
        elif test == 'write latency':
            write_latency.append([total, num, hostname, unit])
        f.close()

    test_results = [randread_bandwidth, randread_iops, randread_latency,
                    randwrite_bandwidth, randwrite_iops, randwrite_latency,
                    read_bandwidth, read_iops, read_latency, write_bandwidth,
                    write_iops, write_latency]
    test_names = ['Randread Bandwidth', 'Randread IOPS', 'Randread Latency',
                  'Randwrite Bandwidth', 'Randwrite IOPS', 'Randwrite Latency',
                  'Read Bandwidth', 'Read IOPS', 'Read Latency',
                  'Write Bandwidth', 'Write IOPS', 'Write Latency']
    return (test_results, test_names, hosts)

def rados_data(rados):
    hosts = []
    sequential_bandwidth = []
    sequential_latency = []
    test_names = []
    test_results = []
    write_bandwidth = []
    write_latency = []

    for filename in rados:
        f = open(filename, 'r')
        total, num, unit = calculate_average(f)
        file_split = filename.split('/')[-1].split('.')
        test_type = filename.split('/')[-1].split('_')
        if len(file_split) > 1:
            hostname = file_split[1].split('_')[0]
            if hostname not in hosts:
                hosts.append(hostname)
        else:
            f.close()
            continue
        test = ' '.join(test_type[3:5]).split('.')
        test = test[0] + ' ' + test[1].split(' ')[1]
        if test == 'write bandwidth':
            write_bandwidth.append([total, num, hostname, unit])
        elif test == 'write latency':
            write_latency.append([total, num, hostname, unit])
        elif test == 'seq bandwidth':
            sequential_bandwidth.append([total, num, hostname, unit])
        elif test == 'seq latency':
            sequential_latency.append([total, num, hostname, unit])
        f.close()

    test_results = [write_bandwidth, write_latency, sequential_bandwidth,
                    sequential_latency]
    test_names = ['Write Bandwidth', 'Write Latency', 'Sequential Bandwidth',
                  'Sequential Latency']

    return (test_results, test_names, hosts)

def print_results(test_results, test_names, hosts, overall):
    overall_results = {}
    results_units = {}

    for hostname in sorted(hosts):
        if not overall:
            print '---%s---' % (hostname)
        total = 0.0
        num = 0.0
        iteration = 0
        for test_type in test_results:
            unit = ''
            for test in test_type:
                if hostname == test[2]:
                    total += test[0]
                    num += test[1]
                    unit = test[3]
            if num > 0.0:
                name = test_names[iteration]
                if not overall:
                    print '  %s: %s %s' % (name, float(total/num), unit)
                if test_names[iteration] in overall_results:
                    overall_results[name] = float(overall_results[name]) +\
                                            float(total/num)
                else:
                    overall_results[name] = float(total/num)
                    results_units[name] = unit
            total = 0.0
            num = 0.0
            iteration += 1
        if not overall:
            print

    print '-------- Overall --------'
    for key, value in sorted(overall_results.items()):
        print '  %s: %s %s' % (key, float(value/len(hosts)), results_units[key])
    print

def iterate_files(kvmrbd, rados, rbd, overall):
    test_results, test_names, hosts = rbd_data(kvmrbd)

    print
    print '----------------------------------------'
    print ' KvmRbdFio Averages'
    print '----------------------------------------'
    print

    print_results(test_results, test_names, hosts, overall)

    test_results, test_names, hosts = rados_data(rados)

    print
    print '----------------------------------------'
    print ' Rados Averages'
    print '----------------------------------------'
    print

    print_results(test_results, test_names, hosts, overall)

    test_results, test_names, hosts = rbd_data(rbd)

    print
    print '----------------------------------------'
    print ' RbdFio Averages'
    print '----------------------------------------'
    print

    print_results(test_results, test_names, hosts, overall)

def exit_failure(message, code):
    print message
    sys.exit(code)


if __name__ == "__main__":
    args = process_arguments()
    kvmrbd, rados, rbd = get_files(args.data_directory)
    iterate_files(kvmrbd, rados, rbd, args.overall_only)
    sys.exit(EXIT_SUCCESS)
