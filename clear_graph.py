"""
Command-line script for posting RDF files to the VIVO API.
"""

import argparse
import os
import time

from rdflib import Graph, URIRef, XSD, Literal
from rdflib.query import ResultException

from lib import backend
from settings import logger

NG_BASE = "http://localhost/data/"


def process(named_graph, batch, dry=False, sleep=10):
    while True:
        vstore = backend.get_store()
        logger.info("Querying {} for triples to remove.".format(named_graph))
        q = """
        CONSTRUCT {
            ?s ?p ?o .
        }
        WHERE {
            GRAPH <?g> {
                ?s ?p ?o .
            }
        }
        LIMIT ?batch
        """.replace("?g", named_graph).replace("?batch", batch)
        logger.info(q)
        try:
            rsp = vstore.query(q)
            g = rsp.graph
            num_found = len(g)
        except ResultException:
            logger.info("No triples to remove.")
            break
        logger.info("Removing {} triples from {}.".format(num_found, named_graph))
        rm = vstore.bulk_remove(named_graph, g)
        #if num_found < batch:
        #    break
        if sleep > 0:
            logger.info("Sleeping between batches.")
            time.sleep(sleep)

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load triples')
    parser.add_argument('--dry-run', '-d', action="store_true", dest="dry", default=False, help="Dry run.")
    parser.add_argument('--sleep', '-sp', action="store", default=0, type=int)
    parser.add_argument('--graph', '-g', required=True)
    parser.add_argument('--batch', '-b', default='4000', type=str)
    args = parser.parse_args()
    done = process(args.graph, args.batch, dry=args.dry, sleep=args.sleep)
