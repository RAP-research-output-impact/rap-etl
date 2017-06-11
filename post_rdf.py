"""
Command-line script for posting RDF files to the VIVO API.
"""

import argparse
import os
from Queue import Queue
from threading import Thread
import time

from rdflib import Graph

from lib import backend
from settings import logger

NG_BASE = "http://localhost/data/"


class ThreadedLoad(object):
    """
    Post a set of RDF files to the VIVO API using threads.
    """

    def __init__(self, triple_files, dry=False, sync=False, threads=1, sleep=10):
        self.triple_files = triple_files
        self.threads = threads
        self.dry = dry
        self.sleep = sleep
        self.sync = sync

    def process(self, fpath, format="nt"):
        g = Graph()
        g.parse(source=fpath, format=format)
        named_graph = NG_BASE + fpath.split("/")[-1].split(".")[0]
        logger.info("Processing updates with {} triples to {}.".format(len(g), named_graph))
        if self.dry is True:
            logger.info("Dry run. No changes made.")
        else:
            if self.sync is True:
                logger.info("Syncing graph to {}.".format(named_graph))
                added, removed = backend.sync_updates(named_graph, g)
            else:
                logger.info("Posting graph as updates to {}.".format(named_graph))
                added, removed = backend.post_updates(named_graph, g)
            if (added > 0) or (removed > 0):
                logger.info("Sleeping for {} seconds between files.".format(self.sleep))
                time.sleep(self.sleep)
            else:
                logger.info("No changes made to {}.".format(named_graph))

    def _service(self, num, harvest_q):
        """thread worker function"""
        while True:
            fpath = harvest_q.get()
            logger.info('Worker: %s. Set: %s' % (num, fpath))
            value = self.process(fpath)
            harvest_q.task_done()

    def run_post(self):
        num_fetch_threads = self.threads
        logger.info("Threaded load with {} threads.".format(self.threads))
        _queue = Queue()
        # Set up some threads to fetch the enclosures
        for i in range(num_fetch_threads):
            worker = Thread(target=self._service, args=(i, _queue,))
            worker.setDaemon(True)
            worker.start()

        for fname in self.triple_files:
            _queue.put(fname)

        logger.debug('Load initialized')
        _queue.join()
        logger.info("Threads complete.")


def verify(paths):
    for fname in paths:
        if os.path.exists(fname) is not True:
            raise Exception("{} does not exist.".format(fname))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load triples')
    parser.add_argument('--dry-run', '-d', action="store_true", dest="dry", default=False, help="Dry run.")
    parser.add_argument('--sync', '-s', action="store_true", dest="sync", default=False, help="Do graph sync rather than update.")
    parser.add_argument('--path', '-p', action="store", nargs='*')
    parser.add_argument('--threads', '-t', action="store", default=1, type=int)
    #parser.add_argument('--sleep', '-s', action="store", default=10, type=int)
    args = parser.parse_args()
    verify(args.path)
    th = ThreadedLoad(args.path, dry=args.dry, sync=args.sync, threads=args.threads)
    th.run_post()
