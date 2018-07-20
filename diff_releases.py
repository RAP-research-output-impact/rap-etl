"""
Command-line script for comparing RDF from one version
to the next and saving RDF files of additions and removals
to the staging directory.
"""

import argparse
import glob
import os
import time

from rdflib import Graph
from rdflib.util import guess_format
from rdflib.compare import graph_diff

from lib import utils
from settings import logger


def process(release, previous):
    current_path = utils.get_rdf_path(release)
    previous_path = utils.get_rdf_path(previous)
    staging_path = utils.get_staging_path(release)
    add_path = utils._do_paths(staging_path, 'add')
    remove_path = utils._do_paths(staging_path, 'delete')

    for rfile in glob.glob(os.path.join(current_path, '*.nt')):
        current_g = Graph().parse(rfile, format="nt")
        logger.info("Processing {}. Found {} incoming triples.".format(rfile, len(current_g)))
        fn = os.path.split(rfile)[-1]
        last_file = os.path.join(previous_path, fn)
        if os.path.exists(last_file) is True:
            last_g = Graph().parse(last_file, format="nt")
            logger.info("Processing {}. Found {} incoming triples.".format(last_file, len(last_g)))
        else:
            last_g = Graph()

        both, adds, deletes = graph_diff(current_g, last_g)
        del both

        add_out = os.path.join(add_path, fn)
        logger.info("Serializing {} adds to {}.".format(len(adds), add_out))
        remove_out = os.path.join(remove_path, fn)
        adds.serialize(destination=add_out, format="nt")
        logger.info("Serializing {} deletes to {}.".format(len(deletes), remove_out))
        deletes.serialize(destination=remove_out, format="nt")

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Diff releases')
    parser.add_argument('--release', '-r', type=int, help="Release number")
    parser.add_argument('--previous', '-p', type=int, help="Prevous release number")

    args = parser.parse_args()

    if args.previous is not None:
        previous = args.previous
    else:
        previous = args.release - 1

    process(args.release, previous)


