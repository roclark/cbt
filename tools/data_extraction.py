import errno
import os


# The following are constants for the sub-directories in the filename.
# CBT saves the files in the following format for Radosbench:
# <root_directory>/00000000/<benchmark_name>/<osd_read_ahead>/<object_size>/
#   <concurrent_operations>/<mode>
BENCHMARK = -6
CONCURRENT_OPS = -3
CONCURRENT_OPS_INTS = 15  # Location of the integers for concurrent_ops
MODE = -2
OP_SIZE = -4
OP_SIZE_INTEGERS = 8  # Location of the integers for op_size

# The following format is used for KvmRbdFio and RbdFio:
# <root_directory>/00000000/<benchmark_name>/<osd_read_ahead>/
#   <client_read_ahead>/<object_size>/<concurrent_processes>/<io_depth>/<mode>
FIO_BENCHMARK = -8
FIO_CONCURRENT_PROCS = -4
FIO_CONCURRENT_PROCS_INTS = 17  # Location of the integers for concurrent_procs
FIO_HOSTNAME = -1
FIO_MODE = -2
FIO_OP_SIZE = -5
FIO_OP_SIZE_INTEGERS = 8  # Starting location of the integers for op_size

# Sometimes, two values in seperate columns run together (eg. 3.0321.3214)
# This method splits up the doubled (or tripled) values for proper parsing
_CUR_MBS = 5  # Column number for current MB/s
_DOUBLE_OVERLAP = 3  # True if two categories overlap
_LAST_LAT = 6  # Column number for last latency values
_MIN_COLUMNS = 5  # Minimum number of columns required that show valid data
_TRIPLE_OVERLAP = 4  # True if three categories overlap


def _split_double(line):
    values = line.split()
    if len(values) < _MIN_COLUMNS:
        return None
    split = values[_CUR_MBS].split('.')
    if len(split) == _TRIPLE_OVERLAP:
        # Gets the first number before '.' and everything before the next '.'
        values[_CUR_MBS] = '%s.%s' % (split[0], split[1][:-1])
        values.append('%s.%s' % (split[1][-1], split[2][:-1]))
    elif len(split) == _DOUBLE_OVERLAP:
        values[_CUR_MBS] = '%s.%s' % (split[0], split[1][:-1])
        values.append('%s.%s' % (split[1][-1], split[2]))
    elif len(split) < _DOUBLE_OVERLAP:  # No overlap found
        return values
    else:
        values.append(values[_CUR_MBS])
        values[_LAST_LAT] = '%s.%s' % (split[1][-1], split[2][:-1])


class DataExtraction:
    def parse_data(self, benchmark, filename, fio, mode=None):
        data = []

        if fio:
            lines = open(filename, 'r').readlines()
            for line in lines:
                data.append(line.split(',')[1])
            benchmark.data = data
        else:
            try:
                with open(filename, 'r') as parse_file:
                    # First four lines/final twelve do not contain data
                    benchmark.lines = parse_file.readlines()[4:-12]
                    parse_file.close()
                    for line in benchmark.lines:
                        data_values = _split_double(line)
                        if data_values is not None and mode == 'bandwidth':
                            benchmark.data.append(data_values[5])
                        elif data_values is not None and mode == 'latency':
                            benchmark.data.append(data_values[6])
            except IOError:
                sys.exit(errno.ENDENT)

    def get_out_file(self, benchmark, output_directory, measurement, test):
        if measurement is None:
            out_file = '%s_%s_%s_%s_%s-%s' % (benchmark.benchmark,
                                              benchmark.op_size,
                                              benchmark.concurrent_procs,
                                              benchmark.mode,
                                              test, benchmark.hostname)
        else:
            out_file = '%s_%s_%s_%s_%s' % (benchmark.benchmark,
                                           benchmark.op_size,
                                           benchmark.concurrent_ops,
                                           benchmark.mode,
                                           measurement)
        benchmark.out_file = os.path.join(output_directory, out_file)

    def create_message(self, benchmark, filename, fio):
        if fio:
            directories = filename.split('/')
            bench_name = directories[FIO_BENCHMARK]
            op_size = directories[FIO_OP_SIZE][FIO_OP_SIZE_INTEGERS:]
            cp = directories[FIO_CONCURRENT_PROCS][FIO_CONCURRENT_PROCS_INTS:]
            mode = directories[FIO_MODE]
            hostname = directories[FIO_HOSTNAME].split('.log.')[1]
            benchmark.header += 'using %s.\n' % (bench_name)
            benchmark.header += '# Op Size: %s\n' % (op_size.lstrip('0'))
            benchmark.header += '# Concurrent Ops: %s\n' % (cp.lstrip('0'))
            benchmark.header += '# Mode: %s\n' % (mode)
            benchmark.benchmark = bench_name
            benchmark.op_size = op_size.lstrip('0')
            benchmark.concurrent_procs = cp.lstrip('0')
            benchmark.mode = mode
            benchmark.hostname = hostname
        else:
            directories = filename.split('/')
            bench_name = directories[BENCHMARK]
            op_size = directories[OP_SIZE][OP_SIZE_INTEGERS:].lstrip('0')
            co = directories[CONCURRENT_OPS][CONCURRENT_OPS_INTS:].lstrip('0')
            mode = directories[MODE]
            benchmark.header += 'using %s.\n' % (bench_name)
            benchmark.header += '# Op Size: %s\n' % (op_size)
            benchmark.header += '# Concurrent Ops: %s\n' % (co)
            benchmark.header += '# Mode: %s\n' % (mode)
            benchmark.benchmark = bench_name
            benchmark.op_size = op_size
            benchmark.concurrent_ops = co
            benchmark.mode = mode

