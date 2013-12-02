import os
import lzma
import sys
import json
import argparse
import telemetry.util.timer as timer

from telemetry.persist import StorageLayout
from datetime import datetime
from multiprocessing import Process, Queue, cpu_count
from pymongo import MongoClient
from functools import partial

class MongoImporter:
    def __init__(self, database="telemetry", collection="payloads", drop_db=True):
        self._client = MongoClient(w=0)
        self._db = self._client[database]
        self._coll = self._db[collection]

        self._queue = Queue()
        self._n_workers = cpu_count()

        if drop_db:
            self._coll.drop()

    def import_files(self, input_directory):
        begin = datetime.now()
        processes = []

        self._enqueue_process(partial(self._master, input_directory), processes)
        for worker in range(0, self._n_workers):
            self._enqueue_process(partial(self._worker), processes)

        for p in processes:
            p.join()

        print("Files imported in", timer.delta_sec(begin), "seconds.")

    def _enqueue_process(self, fun, process_list):
        p = Process(target=fun)
        p.start()
        process_list.append(p)

    def _enqueue_filenames(self, input_directory):
        for root, _, files in os.walk(input_directory):
            for f in files:
                if not f.endswith(StorageLayout.COMPRESSED_SUFFIX):
                    continue

                fullpath = os.path.join(root, f)
                self._queue.put(fullpath)

    def _replace_dots(self, payload):
        keys = ["slowSQL", "slowSQLStartup", "addonDetails", "addonHistograms"]

        def tran(json):
            if not json:
                return None

            return {key.replace(".", "[dot]") : (tran(value) if isinstance(value, dict) else value)
                    for key, value in json.items()}

        for key in keys:
            payload[key] = tran(payload.get(key, None))

    def _import_file(self, path):
        try:
            payloads = []

            with lzma.open(path) as f:
                content = f.readlines()
                for line in content:
                    payload = json.loads(line[37:].decode("utf-8"))
                    # Field names cannot contain dots
                    # http://docs.mongodb.org/manual/reference/limits/#Restrictions%20on%20Field%20Names
                    self._replace_dots(payload)
                    payloads.append(payload)

            self._coll.insert(payloads)
            print("inserted ", len(payloads), " payloads")
        except Exception as e:
            print(e)
            pass

    def _master(self, input_directory):
        self._enqueue_filenames(input_directory)

        for worker in range(0, self._n_workers):
            self._queue.put(None)

    def _worker(self):
        while True:
            path = self._queue.get()
            if path == None:
                break

            self._import_file(path)

def main():
    parser = argparse.ArgumentParser(description="Import telemetry payloads in mongodb.",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("input_directory", help="The directory containing the payloads to import.")
    parser.add_argument("-d", "--database", help="Name of the database into which the payloads are going to be imported.", default="telemetry")
    parser.add_argument("-c", "--collection", help="Name of the collection into which the payloads are going to be imported.", default="payloads")
    parser.add_argument("-p", "--drop", help="True if the database should be dropped before importing the payloads.", default=True)
    args = parser.parse_args()

    importer = MongoImporter(args.database, args.collection, args.drop)
    importer.import_files(args.input_directory)

if __name__ == "__main__":
    sys.exit(main())
