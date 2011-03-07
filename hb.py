"""
An abstraction layer for pulling download metrics out of hbase.

The Thrift/Hbase API is a mess only a Java programmer could love. This is not a
complete wrapper; pieces are implemented as needed.

It's assumed that the value of TCells (which are returned as byte arrays)
should be converted to unsigned long longs.

"""
import struct


from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase, ttypes


exceptions = (Thrift.TException, ttypes.IOError, ttypes.IllegalArgument,
              ttypes.AlreadyExists)


def convert(rows):
    """Unpack each value in the TCell to an unsigned long long."""
    # It may be wiser to do this lazily.
    for row in rows:
        columns = row.columns
        for key, tcell in columns.iteritems():
            columns[key] = struct.unpack('!Q', tcell.value)[0]
    return rows


class Client(object):

    def __init__(self, host, port, table):
        self.host = host
        self.port = port
        self.table = table
        self.open()

    def open(self):
        socket = TSocket.TSocket(self.host, self.port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(protocol)
        self.transport.open()

    def close(self):
        self.transport.close()

    def recycle(self):
        self.close()
        self.open()

    def __del__(self):
        self.close()

    def scanner(self, start='', columns=None):
        """Get a new scanner on the table."""
        id = self.client.scannerOpen(self.table, start, columns)
        return Scanner(self, id)

    def row(self, row_, columns=None):
        """Fetch the row_, optionally constrained to a list of columns."""
        rv = self.client.getRowWithColumns(self.table, row_, columns)
        return convert(rv) if rv else []


class Scanner(object):

    def __init__(self, client, id):
        self.client = client
        self.id = id

    def next(self):
        """Fetch the next row from the scanner."""
        return convert(self.client.client.scannerGet(self.id))[0]

    def list(self, num):
        """Fetch the next ``num`` rows from the scanner."""
        return convert(self.client.client.scannerGetList(self.id, num))
