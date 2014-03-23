# This is a (hopefully temporary) shim that uses the rdb protocol to
# implement the memcache API

import contextlib
import rdb_workload_common

@contextlib.contextmanager
def make_memcache_connection(opts):
    with rdb_workload_common.make_table_and_connection(opts) as (table, conn):
        yield MemcacheRdbShim(table, conn)

class MemcacheRdbShim(object):
    def __init__(self, table, conn):
        self.table = table
        self.conn = conn

    def get(self, key):
        response = table.get(key).run(conn)
        if response:
            return response['val']

    def set(self, key, val):
        response = table.insert({
            'id': key,
            'val': val
            },
            upsert=True
            ).run(conn)

        return response['inserted'] | response['replaced']

def option_parser_for_memcache():
    return rdb_workload_common.option_parser_for_connect()
