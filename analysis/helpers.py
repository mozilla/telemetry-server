try:
    import simplejson as json
except ImportError:
    import json
from subprocess import Popen, PIPE
from traceback import print_exc
import sys

def decompress_input(process):
    def wrapper(self, prefix, path):
        # Find dimensions
        dims = prefix.split('/')
        dims += dims.pop().split('.')[:2]

        # Open a compressor
        raw_handle = open(path, "rb")
        decompressor = Popen(
            ['xz', '-d', '-c'],
            bufsize = 65536,
            stdin = raw_handle,
            stdout = PIPE,
            stderr = sys.stderr
        )

        # Process each line
        line_nb = 0
        errors = 0
        for line in decompressor.stdout:
            line_nb += 1
            try:
                uid, payload = line.split("\t", 1)
                process(self, uid, dims, payload)
            except:
                print >> sys.stderr, ("Bad input line: %i of %s" %
                                      (line_nb, prefix))
                print_exc(file = sys.stderr)
                errors += 1

        # Close decompressor
        decompressor.stdout.close()
        raw_handle.close()

        # Return number of failed records
        return errors
    return wrapper

def parse_input(process):
    def wrapper(self, uid, dimensions, payload):
        process(self, uid, dimensions, json.loads(payload))
    return decompress_input(wrapper)

class Processor:
    def __init__(self, output_folder):
        self.output_folder = output_folder

    def process(self, prefix, path):
                 # Raise exception on critical crash error
                 # Print errors to stderr
        return 0 # number of errors (rows we had problems parsing)

    @decompress_input
    def process(self, uid, dimensions, payload):
        pass    # Raise exception on error

    @parse_input
    def process(self, uid, dimensions, json):
        pass    # Raise exception on error

    def flush(self):
        pass
