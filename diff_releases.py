"""
Command-line script for comparing RDF from one version
to the next and saving RDF files of additions and removals
to the staging directory.
"""

import argparse
import glob
import os

from rdflib import Graph
from rdflib.compare import graph_diff

from lib import utils
from settings import logger


def process(incites, release, previous):
    logger.info("Processing release {}. Checking previous release {} for additions and removals.".format(release, previous))
    current_path = utils.get_rdf_path(release)
    previous_path = utils.get_rdf_path(previous)
    staging_path = utils.get_staging_path(release)
    add_path = os.path.join(staging_path, 'add')
    if not os.path.exists(add_path):
        os.mkdir(add_path)
    remove_path = os.path.join(staging_path, 'delete')
    if not os.path.exists(remove_path):
        os.mkdir(remove_path)
    files = '*.nt'
    if incites:
        files = 'incites-*.nt'
    for triple_file in glob.glob(os.path.join(current_path, files)):
        current_g = Graph().parse(triple_file, format="nt")
        logger.info("Processing {}. Found {} incoming triples.".format(triple_file, len(current_g)))
        fn = os.path.split(triple_file)[-1]
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
    parser.add_argument('--incites', '-i', action="store_true", dest="incites", default=False, help="Process incites files only.")

    args = parser.parse_args()

    if args.previous is not None:
        previous = args.previous
    else:
        previous = args.release - 1
        if previous < 0:
            raise Exception("Negative release numbers not allowed. Check release arguments.")

    process(args.incites, args.release, previous)


