"""
Command-line script for posting RDF files to the VIVO API.
"""

import argparse
import os
import time

from rdflib import Graph

from lib import backend
from settings import logger

NG_BASE = "http://localhost/data/"

# Number of triples to post to VIVO SPARQL Update endpoint at one time.
# Larger batches tend to fail with 403 error.
DEFAULT_BATCH_SIZE = 5000


def process(triple_files, format="nt", dry=False, sync=False, sleep=10, size=DEFAULT_BATCH_SIZE):
    vstore = backend.get_store()
    for fpath in triple_files:
        g = Graph()
        g.parse(source=fpath, format=format)
        named_graph = NG_BASE + fpath.split("/")[-1].split(".")[0]
        logger.info("Processing updates with {} triples to {} and batch size {}.".format(len(g), named_graph, size))
        if dry is True:
            logger.info("Dry run. No changes made.")
        else:
            if sync is True:
                logger.info("Syncing graph to {}.".format(named_graph))
                added, removed = backend.sync_updates(named_graph, g, size=size)
            else:
                logger.info("Posting graph as updates to {}.".format(named_graph))
                added = vstore.bulk_add(named_graph, g, size=size)
                removed = 0
            if (added > 0) or (removed > 0):
                if sleep > 0:
                    logger.info("Sleeping for {} seconds between files.".format(sleep))
                    time.sleep(sleep)
            else:
                logger.info("No changes made to {}.".format(named_graph))
    return True


def verify(paths):
    for fname in paths:
        if os.path.exists(fname) is not True:
            raise Exception("{} does not exist.".format(fname))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load triples')
    parser.add_argument('--dry-run', '-d', action="store_true", dest="dry", default=False, help="Dry run.")
    parser.add_argument('--sync', '-s', action="store_true", dest="sync", default=False, help="Do graph sync rather than update.")
    parser.add_argument('--path', '-p', action="store", nargs='*')
    parser.add_argument('--format', '-f', action="store", default="nt")
    parser.add_argument('--sleep', '-sp', action="store", default=0, type=int)
    parser.add_argument('--batch', '-b', action="store", default=DEFAULT_BATCH_SIZE, type=int)
    args = parser.parse_args()
    verify(args.path)
    done = process(args.path, format=args.format, dry=args.dry, sync=args.sync, sleep=args.sleep, size=args.batch)
