import os

from data_extraction import DataExtraction

__all__ = ['RadosBandwidth', 'FIO', 'RadosLatency']


class RadosBandwidth:
    def __init__(self, filename, output_directory, unit):
        data_extraction = DataExtraction()

        self.benchmark = ''
        self.concurrent_ops = ''
        self.data = []
        self.header = '# The following is a list of throughput values in %s '\
                       % (unit)
        self.lines = []
        self.mode = ''
        self.op_size = ''
        self.out_file = ''

        data_extraction.parse_data(self, filename, False, 'bandwidth')
        data_extraction.create_message(self, filename, False)
        data_extraction.get_out_file(self, output_directory, 'bandwidth', None)


class FIO:
    def __init__(self, filename, output_directory, test, header):
        data_extraction = DataExtraction()

        self.benchmark = ''
        self.concurrent_procs = ''
        self.data = []
        self.header = header
        self.hostname = ''
        self.mode = ''
        self.op_size = ''
        self.out_file = ''

        data_extraction.parse_data(self, filename, True)
        data_extraction.create_message(self, filename, True)
        data_extraction.get_out_file(self, output_directory, None, test)


class RadosLatency:
    def __init__(self, filename, output_directory, unit):
        data_extraction = DataExtraction()

        self.benchmark = ''
        self.concurrent_ops = ''
        self.data = []
        self.header = '# The following is a list of latency values in %s '\
                       % (unit)
        self.lines = []
        self.mode = ''
        self.op_size = ''
        self.out_file = ''

        data_extraction.parse_data(self, filename, False, 'latency')
        data_extraction.create_message(self, filename, False)
        data_extraction.get_out_file(self, output_directory, 'latency', None)

