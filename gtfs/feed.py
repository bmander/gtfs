import os
import errno
from zipfile import ZipFile
from util import TolerantDictReader


class Feed(object):
    """A Feed is a collection of CSV files with headers, either zipped into
    an archive or loose in a folder"""

    def __init__(self, filename):
        self.filename = filename
        self.zf = None

        if not os.path.isdir(filename):
            self.zf = ZipFile(filename)

    def get_reader(self, filename):
        if self.zf:
            try:
                f = self.zf.open(filename)
            except KeyError:
                raise FileNotFoundError("%s not found" % filename)
        else:
            try:
                f = open(os.path.join(self.filename, filename))
            except IOError, e:
                if e.errno == errno.ENOENT:
                    raise FileNotFoundError("%s not found" % filename)
                else:
                    raise

        dr = TolerantDictReader(f)
        return dr


class FileNotFoundError(Exception):
    pass
