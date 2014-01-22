"""
Convert sysstat binary data to JSON.
"""
import json
import subprocess
import sys
import tempfile

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

DATA_COMPAT = 'Data file format is not compatible'
FILE_READ_FAIL = 'Unable to read {}'
FILE_WRITE_FAIL = 'Unable to write {}'

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
        self._sadf = ['sadf', '-j', '--']
        self.data = None

    def convert(self, interval=1):
        """
        Convert sysstat binary data to a text format.

        Specify sample interval in seconds (default is 1).
        """
        self._build_sadf_command(interval)
        self._run_sadf()
        self._parse_json()

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
        
    @property
    def version(self):
        return self._data_version
    
    @property
    def hostname(self):
        return self._host['nodename']
    
    @property
    def date(self):
        return self._host['file-date']
    
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

    def _run_sadf(self):
        """
        Run the sadf command in a subprocess and let it write to a spooled file.
        """
        subprocess.check_call(self._sadf, stdout=self._out)
