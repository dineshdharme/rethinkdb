
def module(module):
    __import__(module, level=0)
    return sys.modules[module]
