# TODO : test that depend on each other
# TODO : parameterised tests

import multiprocessing
import threading
import signal
from argparse import ArgumentParser
import sys
import tempfile
from os.path import abspath, join, dirname, pardir
import os
import traceback
import subprocess
import time
import traceback

parser = ArgumentParser(description='Run RethinkDB tests')
parser.add_argument('-j', '--jobs', type=int, default=1,
                    help='The number of tests to run simultaneously')
parser.add_argument('-l', '--list', dest='mode', action='store_const', const='list')
parser.add_argument('-o', '--output-dir')
parser.add_argument('-r', '--repeat', type=int, default=1,
                    help='The number of times to repeat each test')
parser.add_argument('-k', '--continue', action='store_true', dest='kontinue',
                    help='Continue repeating even if a test fails')
parser.add_argument('-a', '--abort-fast', action='store_true', dest='abort_fast',
                    help='Abort the tests when a test fails')
parser.add_argument('-v', '--verbose', action='store_true')
parser.add_argument('-t', '--timeout', type=int, default=600,
                    help='Timeout in seconds for each test')
parser.add_argument('filter', nargs='*',
                    help='The name of the tests to run, or a group'
                    'or tests or their negation with !')

def run(all_tests, args):
    args = parser.parse_args(args)
    filter = TestFilter.parse(args.filter)
    tests = all_tests.filter(filter)
    reqs = tests.requirements()
    conf = configure(reqs)
    tests = tests.configure(conf)
    filter.check_use()
    if args.mode == 'list':
        for name, __ in tests:
            print name
        return
    else:
        testrunner = TestRunner(
            tests, conf,
            tasks=args.jobs,
            timeout=args.timeout,
            output_dir=args.output_dir,
            verbose=args.verbose,
            repeat=args.repeat,
            kontinue=args.kontinue,
            abort_fast=args.abort_fast)
        testrunner.run()

def configure(reqs):
    # TODO
   return dict(
       SRC_ROOT = abspath(join(dirname(__file__), pardir, pardir)))

def redirect_fd_to_file(fd, file, tee=False):
    if not tee:
        f = open(file, 'w')
    else:
        tee = subprocess.Popen(["tee", file], stdin=subprocess.PIPE)
        f = tee.stdin
    os.dup2(f.fileno(), fd)
    

class TestRunner(object):
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    TIMED_OUT = 'TIMED_OUT'
    STARTED = 'STARTED'
    
    def __init__(self, tests, conf, tasks=1, timeout=600, output_dir=None, verbose=False, repeat=1, kontinue=False, abort_fast = False):
        self.tests = tests
        self.semaphore = multiprocessing.Semaphore(tasks)
        self.processes = []
        self.timeout = timeout
        self.conf = conf
        self.verbose = verbose
        self.repeat = repeat
        self.kontinue = kontinue
        self.failed_set = set()
        self.aborting = False
        self.abort_fast = abort_fast

        if output_dir:
            self.dir = output_dir
            try:
                os.mkdir(output_dir)
            except OSError as e:
                print >> sys.stderr, "Could not create output directory (" + output_dir + "):", e
                sys.exit(1)
        else:
            tr_dir = join(conf['SRC_ROOT'],'build/test_results/') 
            try:
                os.makedirs(tr_dir)
            except OSError:
                pass
            timestamp = time.strftime('%Y-%m-%dT%H:%M:%S.')
            self.dir = tempfile.mkdtemp('', timestamp, tr_dir)
        
        self.running = Locked({})
        self.view = TermView(self) if sys.stdout.isatty() and not verbose else TextView(self)
        
    def run(self):
        tests_count = len(self.tests)
        tests_launched = 0
        try:
            print "Running %d tests (output_dir: %s)" % (tests_count, self.dir)

            for i in range(0, self.repeat):
                for name, test in self.tests:
                    if self.aborting:
                        break
                    self.semaphore.acquire()
                    if self.aborting:
                        self.semaphore.release()
                        break
                    if self.kontinue or name not in self.failed_set:
                        id = (name, i)
                        dir = join(self.dir, name if self.repeat == 1 else name + '.' + str(i+1)) 
                        process = TestProcess(self, id, test, dir)
                        with self.running as running:
                            running[id] = process
                        tests_launched = tests_launched + 1
                        process.start()
                    else:
                        self.semaphore.release()

            self.wait_for_running_tests()

        except:
            self.aborting = True
            (exc_type, exc_value, exc_trace) = sys.exc_info()
            if not exc_type == KeyboardInterrupt:
                print
                print '\n'.join(traceback.format_exception(exc_type, exc_value, exc_trace))
            print >>sys.stderr, "\nWaiting for tests to finish..."
            try:
                self.wait_for_running_tests()
            except:
                print "Killing remaining tasks..."
                with self.running as running:
                    for id, process in running.iteritems():
                        process.terminate()
        self.view.close()
        if tests_launched != tests_count:
            if len(self.failed_set):
                print "%d tests failed" % (len(self.failed_set),)
            print "%d tests skipped" % (tests_count - tests_launched,)
        elif len(self.failed_set):
            print "%d of %d tests failed" % (len(self.failed_set), tests_count)
        else:
            print "All tests passed successfully"
        print "Saved test results to %s" % (self.dir,)

    def wait_for_running_tests(self):
        # loop through the remaining TestProcesses and wait for them to finish
        while True:
            with self.running as running:
                if not running:
                    break
                id, process = running.iteritems().next()
            process.join()
            with self.running as running:
                try: 
                    del(running[id])
                except KeyError:
                    pass
                else:
                    process.write_fail_message("Test failed to report success or"
                                               " failure status") 
                    self.tell(self.FAILED, id)
        
    def tell(self, status, id, testprocess):
        name = id[0]
        args = {}
        if status == 'FAILED':
            if not self.aborting:
                args = dict(error = testprocess.tail_error())
            if self.abort_fast:
                self.aborting = True
        if status != 'STARTED': 
            with self.running as running:
                del(running[id])
            if status != 'SUCCESS':
                self.failed_set.add(name)
            self.semaphore.release()
        self.view.tell(status, name, **args)

    def count_running(self):
        with self.running as running:
            return len(running)

class TextView(object):
    green = "\033[32;1m"
    red = "\033[31;1m"
    nocolor = "\033[0m"
        
    def __init__(self, runner):
        self.runner = runner
        self.use_color = sys.stdout.isatty()

    def tell(self, event, name, **args):
        if event != 'STARTED':
            print self.format_event(event, name, **args)

    def format_event(self, str, name, error=None):
        short = dict(
            FAILED = (self.red, "FAIL"),
            SUCCESS = (self.green, "OK  "),
            TIMED_OUT = (self.red, "TIME")
        )[str]
        buf = ''
        if error:
            buf += error + '\n'
        if self.use_color:
            buf += short[0] + short[1] + " " + name + self.nocolor
        else:
            buf += short[1] + " " + name
        return buf

    def close(self):
        pass

class TermView(TextView):
    def __init__(self, runner):
        TextView.__init__(self, runner)
        self.running_list = []
        self.buffer = ''
        self.read_pipe, self.write_pipe = multiprocessing.Pipe(False)
        self.thread = threading.Thread(target=self.run, name='TermView')
        self.thread.daemon = True
        self.thread.start()

    def tell(self, *args, **kwargs):
        self.write_pipe.send((args, kwargs))

    def close(self):
        self.write_pipe.send(('EXIT',None))
        self.thread.join()
        
    def run(self):
        while True:
            args, kwargs = self.read_pipe.recv()
            if args == 'EXIT':
                break
            self.thread_tell(*args, **kwargs)
        
    def thread_tell(self, event, name, **kwargs):
        if event == 'STARTED':
            self.running_list += [name]
            self.update_status()
        else:
            self.running_list.remove(name)
            if event == 'SUCCESS':
                color = self.green
            else:
                color = self.red
            self.show(self.format_event(event, name, **kwargs))
        self.flush()

    def update_status(self):
        self.clear_status()
        self.show_status()

    def clear_status(self):
        self.buffer += "\033[0E\033[K"

    def show_status(self):
        if self.running_list:
            self.buffer += '[%d tests running: %s]' % (len(self.running_list), self.format_running())

    def format_running(self):
        ret = self.running_list[0]
        if len(self.running_list) > 1:
            ret += ", ..."
        return ret
            
    def show(self, line):
        self.clear_status()
        self.buffer += line + "\n"
        self.show_status()

    def flush(self):
        sys.stdout.write(self.buffer)
        self.buffer = ''
        sys.stdout.flush()
        
class Locked(object):
    def __init__(self, value=None):
        self.value = value
        self.lock = threading.Lock()

    def __enter__(self):
        self.lock.acquire()
        return self.value

    def __exit__(self, e, x, c):
        self.lock.release()
            
class TestProcess(object):
    def __init__(self, runner, id, test, dir):
        self.runner = runner
        self.id = id
        self.name = id[0]
        self.test = test
        self.timeout = test.timeout() or runner.timeout
        self.supervisor = None
        self.process = None
        self.dir = dir

    def start(self):
        try:
            self.runner.tell(TestRunner.STARTED, self.id, self)
            os.mkdir(self.dir)
            self.supervisor = threading.Thread(target=self.supervise,
                                               name="supervisor:"+self.name)
            self.supervisor.daemon = True
            self.supervisor.start()
        except Exception:
            raise

    def run(self, write_pipe):
        sys.stdin.close()
        redirect_fd_to_file(1, join(self.dir, "stdout"), tee=self.runner.verbose)
        redirect_fd_to_file(2, join(self.dir, "stderr"), tee=self.runner.verbose)
        os.chdir(self.dir)
        with Timeout(self.timeout):
            try:
                self.test.run()
            except TimeoutException:
                write_pipe.send(TestRunner.TIMED_OUT)
            except Exception as e:
                # sys.stderr.write(traceback.format_exc() + '\n')
                print >>sys.stderr, e
                write_pipe.send(TestRunner.FAILED)
            else:
                write_pipe.send(TestRunner.SUCCESS)

    def write_fail_message(self, message):
        with open(join(self.dir, "stderr"), 'a') as file:
            file.write(message)

    def tail_error(self):
        with open(join(self.dir, "stderr")) as f:
            lines = f.read().split('\n')[-10:]
        if len(lines) < 10:
            with open(join(self.dir, "stdout")) as f:
                lines = f.read().split('\n')[-len(lines):] + lines 
        return '\n'.join(lines)
            
            
    def supervise(self):
        read_pipe, write_pipe = multiprocessing.Pipe(False)
        self.process = multiprocessing.Process(target=self.run, args=[write_pipe],
                                               name="subprocess:"+self.name)
        self.process.start()
        self.process.join(self.timeout + 5)
        if self.process.is_alive():
            self.process.terminate()
            self.write_fail_message("Test failed to exit after timeout of %d seconds"
                                        % (self.timeout,))
            self.runner.tell(TestRunner.FAILED, self.id, self)
        elif self.process.exitcode:
            self.write_fail_message("Test exited abnormally with error code %d"
                                        % (self.process.exitcode,))
            self.runner.tell(TestRunner.FAILED, self.id, self)
        else:
            try:
                write_pipe.close()
                status = read_pipe.recv()
            except EOFError:
                self.write_fail_message("Test did not fail, but"
                                        " failed to report its success")
                status = TestRunner.FAILED
            self.runner.tell(status, self.id, self)
                
    def join(self):
        self.supervisor.join()

    def terminate(self):
        if self.process:
            self.process.terminate()

class TimeoutException(Exception):
    pass
        
class Timeout(object):
    def __init__(self, seconds):
        self.timeout = seconds

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.alarm)
        signal.alarm(self.timeout)

    def __exit__(self, type, exception, trace):
        signal.alarm(0)

    @staticmethod
    def alarm(*ignored):
        raise TimeoutException()
        
class TestFilter(object):
    INCLUDE = 'INCLUDE'
    EXCLUDE = 'EXCLUDE'

    def __init__(self, default=EXCLUDE):
        self.default = default
        self.tree = {}
        self.was_matched = False

    @classmethod
    def parse(self, args):
        if not args:
            return TestFilter(self.INCLUDE)
        if args[0][0] == '!':
            filter = TestFilter(self.INCLUDE)
        else:
            filter = TestFilter()
        for arg in args:
            if arg[0] == '!':
                arg = arg[1:]
                type = self.EXCLUDE
            else:
                type = self.INCLUDE
            filter.at(arg.split('.')).reset(type)
        return filter

    def at(self, path):
        if not path:
            return self
        else:
            return self.zoom(path[0], create=True).at(path[1:])

    def reset(self, type=EXCLUDE):
        self.default = type
        self.tree = {}

    def match(self):
        self.was_matched = True
        return self.default == self.INCLUDE

    def zoom(self, name, create=False):
        try:
            return self.tree[name]
        except KeyError:
            subfilter = TestFilter(self.default)
            if create:
                self.tree[name] = subfilter
            return subfilter
        
    def check_use(self, path=[]):
        if not self.was_matched:
            raise Exception('No such test %s' % '.'.join(path))
        for name, filter in self.tree.iteritems():
            filter.check_use(path + [name])

    def __repr__(self):
        return ("TestFilter(" + self.default + ", " + repr(self.was_matched) +
                ", " + repr(self.tree) + ")")

    def all_same(self):
        self.was_matched = True
        return not self.tree
            
class Test(object):
    def __init__(self, timeout=None):
        self._timeout = timeout

    def run(self):
        raise Exception("run is not defined for the %s class" %
                        (type(self).__name__,))

    def filter(self, filter):
        if filter.match():
            return self
        else:
            return None

    def __iter__(self):
        yield (None, self)

    def timeout(self):
        return self._timeout

    def requirements(self):
        return []

    def configure(self, conf):
        return self

class SimpleTest(Test):
    def __init__(self, run, **kwargs):
        Test.__init__(self, **kwargs)
        self._run = run 

    def run(self):
        self._run()
        
class TestTree(Test):
    def __init__(self, tests={}):
        self.tests = dict(tests)

    def filter(self, filter):
        if filter.all_same():
            if filter.match():
                return self
            else:
                return TestTree()
        trimmed = TestTree()
        for name, test in self.tests.iteritems(): 
            subfilter = filter.zoom(name)
            trimmed[name] = test.filter(subfilter)
        return trimmed

    def run(self):
        for test in self.tests.values():
            test.run()

    def __getitem__(self, name):
        return self.tests[name]

    def __setitem__(self, name, test):
        if not test or (isinstance(test, TestTree) and not test.tests):
            try:
                del(self.tests[name])
            except KeyError:
                pass
        else:
            self.tests[name] = test
                
    def __iter__(self):
        for name in sorted(self.tests.keys()):
            for subname, test in self.tests[name]:
                if subname:
                    yield (name + '.' + subname, test)
                else:
                    yield name, test

    def requirements(self):
        for test in self.tests.values():
            for req in test.requirements():
                yield req        
                    
    def configure(self, conf):
        return TestTree((
            (name, test.configure(conf))
            for name, test
            in self.tests.iteritems()
        ))

    def __len__(self):
        count = 0
        for __, ___ in self:
            count += 1
        return count

    def has_test(self, name):
        return self.tests.has_key(name)
