#!/usr/bin/python
"""
Utility for creating preferred names from spreadsheet
for wos:SubOrganizations in rap.
"""

import argparse
import sys
import os
from collections import defaultdict
from lib import utils

from rdflib import Literal, Graph
from namespaces import D, WOS, RDF, RDFS, VIVO, rq_prefixes, GEO

from lib import backend
from settings import (
    logger,
    ADDRESS_COUNTRY_GRAPH,
    RDF_PATH
)
from publications import slug_uri
import warnings

def local_name(uri):
    return uri.split('/')[-1]

def save_rdf(release, graph, ng):
    name = local_name(ng)
    path = os.path.join(RDF_PATH, '{:03d}'.format(release), name + '.nt')
    if os.path.isfile(path):
        logger.info("Appending {} triples to '{}'.".format(len(graph), path))
        file = open(path, 'a') 
    else:
        logger.info("Storing {} triples to '{}'.".format(len(graph), path))
        file = open(path, 'w') 
    file.write(graph.serialize(destination=None, format='nt'))
    file.close()
    return path

def main(release):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        vstore = backend.get_store()
    ct = rq_prefixes + """
        CONSTRUCT {
            ?orgAddress geo:codeISO3 ?code 
        } WHERE {
            ?country a vivo:Country .
            ?country geo:codeISO3 ?code .           
            ?org obo:RO_0001025 ?country . 
            ?org a wos:UnifiedOrganization .                     
            ?org vivo:relatedBy ?orgAddress .
            ?orgAddress a wos:Address .
            FILTER (?org != d:org-technical-university-of-denmark)
        }
    """
    graph = vstore.query(ct).graph
    save_rdf(release, graph, ADDRESS_COUNTRY_GRAPH)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch Incites Data')
    parser.add_argument("--release", type=int)
    args = parser.parse_args()
    utils.release(args.release)
    if utils.RELEASE == 0:
        raise Exception("fatal: release not found: {}".format(args.release))
    main(utils.RELEASE)
