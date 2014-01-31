import logging, sys

default = logging.getLogger('default')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
default.addHandler(ch)
