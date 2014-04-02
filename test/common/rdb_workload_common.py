from vcoptparse import *
import contextlib
import rethinkdb as r
import http_admin

def option_parser_for_connect():
    op = OptParser()
    op['address'] = StringFlag('--address', 'localhost:28015')
    op['table'] = StringFlag('--table')
    return op

@contextlib.contextmanager
def make_table_and_connection(opts):
    (host, port) = opts['address'].split(':')
    with r.connect(host, int(port)) as conn:
        (db, table) = opts['table'].split('.')
        yield (r.db(db).table(table), conn)

def insert_many(host="localhost", port=28015, db="test", table=None, count=10000):
    conn = r.connect(host, port)
    batch_size = 1000

    def gen(i):
        return { 'val': "X" * (i % 100) }

    for start in range(0, count, batch_size):
        end = min(start + batch_size, count)
        res = r.db(db).table(table).insert([gen(i) for i in range(start, end)]).run(conn)
        assert res['inserted'] == end - start
