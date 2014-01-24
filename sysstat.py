"""
Convert sysstat binary data to JSON.
"""
import json
import subprocess
import sys
import tempfile
import time

# Just do JSON for now.  Leaving this here in case I need it later.
#FORMAT_LINE = 0
#FORMAT_JSON = 1
#FORMAT_XML = 2
# Map the constants to sadf command options
#sadf_opts = ['-p', '-j', '-x']

# sar options
PAGING      = '-B'
IO          = '-b'
BLOCK       = '-d'
# These cause "Invalid type of persistent device name"
DEV_PERSIST = '-j'
DEV_LABEL   = 'LABEL'
DEV_PATH    = 'PATH'
NET_DEV     = '-n'
NET_DEV_N   = 'DEV,EDEV'
CPU_STAT    = '-P'
CPU_STAT_N  = 'ALL'
CPU_UTIL    = '-u'
SCHEDULER   = '-q'
TASKS       = '-w'
MEM_STAT    = '-R'
MEM_UTIL    = '-r'
SWAP_STAT   = '-W'
SWAP_UTIL   = '-S'

DATA_COMPAT_ERR = 'Data file format is not compatible'
NO_DATA_ERR     = 'Object has no data'

ISO_8601_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

DEVICE_TAG = {
    'disk' : 'disk-device',
    'network' : 'iface',
    'cpu-load' : 'cpu'
}

class TimeSeriesError(Exception):
    pass

class TimeSeries(object):
    """
    A collection of time series data points from sysstat.
    """
    def __init__(self, infile):
        """
        Construct TimeSeries for infile.  Result will be written to outfile
        (default: stdout)
        """
        self.infile = infile
        self._out = tempfile.SpooledTemporaryFile()
        # could be a constant but might add XML or other options later...
        self._sadf = ['sadf', '-j', '--']
        self._alldata = None
        self._tsdata = None
        self._host = None
        self._data_version = None
        self._unix_time = None
        self._offset_time = None

    def _build_sadf_command(self, interval):
        """
        Build the sadf command, which takes its own options plus sar
        options to filter the data.
        """
        # FIXME make sar options somewhat selectable
        sar_opts = [
            PAGING, IO, BLOCK, NET_DEV, NET_DEV_N, CPU_STAT, CPU_STAT_N,
            CPU_UTIL, SCHEDULER, TASKS, MEM_STAT, MEM_UTIL, SWAP_STAT,
            SWAP_UTIL ]

        # build the sadf command with sar options after --
        for opt in sar_opts:
            self._sadf.append(opt)
        self._sadf.append('{}'.format(interval))
        self._sadf.append('{}'.format(self.infile))

    def _run_sadf(self):
        """
        Run the sadf command in a subprocess and let it write to a spooled file.
        """
        subprocess.check_call(self._sadf, stdout=self._out)

    def _parse_json(self):
        """
        Load a JSON sysstat data from the tempfile that sadf wrote to.  
        """
        self._out.seek(0)
        self._alldata = json.load(self._out)
        self._data_version = self._alldata['sysstat']['sysdata-version']
        # the hosts key is a list but should only ever contain one element
        self._host = self._alldata['sysstat']['hosts'][0]
        self._tsdata = self._host['statistics']

    def convert(self, interval=1):
        """
        Convert sysstat binary data to a text format.

        Specify sample interval in seconds (default is 1).
        """
        self._build_sadf_command(interval)
        self._run_sadf()
        self._parse_json()
        
    def dump(self, out=sys.stdout):
        """
        Dump JSON format.  Output must be an open file descriptor (default is stdout).
        """
        if self._alldata is None:
            raise TimeSeriesError(NO_DATA_ERR)
        out.write(json.dumps(self._alldata, indent=4, sort_keys=False))

    def _string_to_unix_time(self, sdate, stime):
        # convert ISO 8601 date/time strings to unix time integer
        datetime = '{} {}'.format(sdate, stime)
        itime = time.mktime(time.strptime(datetime, ISO_8601_TIME_FORMAT))
        return int(itime)
        
    def _get_unix_times(self):
        # return a list of timestamps in unix (integer) format
        self._unix_time = []
        for datapoint in self._tsdata:
            ts = datapoint['timestamp']
            itime = self._string_to_unix_time(ts['date'], ts['time'])
            self._unix_time.append(itime)

    def _get_offset_times(self):
        # return a list of timestamps in integer (second) offsets
        if not self._unix_time:
            self._get_unix_times()
        self._offset_time = []
        start = self._unix_time[0]
        for t in self._unix_time:
            self._offset_time.append(t - start)

    def _get_simple_series(self, node_path):
        search_class, search_metric = node_path.split('/')
        series = []
        for datapoint in self._tsdata:
            series.append(datapoint[search_class][search_metric])
        return series
        
    def _get_device_series(self, node_path):
        # define device tag names for each device class
        search_class, search_dev, search_metric = node_path.split('/')
        series = []
        for datapoint in self._tsdata:
            devices = datapoint[search_class]
            for device in devices:
                if device[DEVICE_TAG[search_class]] == search_dev:
                    series.append(device[search_metric])
        return series
        
    def _get_subclass_device_series(self, node_path):
        search_class, search_subclass, search_dev, search_metric = node_path.split('/')
        series = []
        for datapoint in self._tsdata:
            devices = datapoint[search_class][search_subclass]
            for device in devices:
                if device[DEVICE_TAG[search_class]] == search_dev:
                    series.append(device[search_metric])
        return series
            
    def get_metrics(self, node_path):
        """
        Get a list of values for a single metric.
        
        The path is similar to a directory path.  First-level paths are metric
        classifications within each datapoint (queue, disk, etc).  Lower levels
        depend on metric class ('queue; contains leaf-nodes only, 'disk' is a 
        list of leaf groups, and 'io' contains a mixture.  Think Xpath.
        
        The JSON and XML formats generated by sadf are not optimal, IMO.  And
        the two vary in ways that don't make sense.
        """
        nelem = len(node_path.split('/'))
        if nelem == 2:
            return self._get_simple_series(node_path)
        elif nelem == 3:
            return self._get_device_series(node_path)
        elif nelem == 4:
            return self._get_subclass_device_series(node_path)
    
    @property
    def unix_times(self):
        if not self._unix_time:
            self._get_unix_times()
        return self._unix_time
    
    @property
    def offset_times(self):
        if not self._offset_time:
            self._get_offset_times()
        return self._offset_time
            
    @property
    def datapoints(self):
        return self._tsdata
    
    @property
    def version(self):
        return self._data_version
    
    @property
    def hostname(self):
        return self._host['nodename']
    
    @property
    def date(self):
        return self._host['file-date']
    
    
