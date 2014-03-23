from vcoptparse import *
import contextlib
import rethinkdb as r

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
    
