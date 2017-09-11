"""
Utility for creating preferred names from spreadsheet
for wos:SubOrganizations in rap.
"""

import argparse
import sys
import csv
from collections import defaultdict

from rdflib import Literal, Graph
from namespaces import D, WOS, RDF, RDFS, VIVO, rq_prefixes


from lib import backend
from settings import (
    logger,
    ADDRESS_GRAPH,
    SUBORG_GRAPH,
    CLEAN_SUBORG_GRAPH
)
from publications import slug_uri


def get_existing_address(uri):
    vstore = backend.get_store()
    rq = rq_prefixes + """
    SELECT ?address
    WHERE {
        ?uri vivo:relatedBy ?address.
        ?address a wos:Address .
    }
    """
    rmg = Graph()
    addr_uris = []
    for row in vstore.query(rq, initBindings={'uri': uri}):
        addr_uris.append(row.address)
        rmg.add((row.address, VIVO.relates, uri))
        rmg.add((uri, VIVO.relatedBy, row.address))
    return addr_uris, rmg


def index_orgs(name_key):
    uri_pubs = defaultdict(list)
    idx = defaultdict(lambda: defaultdict(dict))
    vstore = backend.get_store()

    rmg = Graph()
    addg = Graph()

    q = rq_prefixes + """
    SELECT DISTINCT ?org ?name
    WHERE {
        ?org a wos:SubOrganization ;
            rdfs:label ?label ;
            wos:subOrganizationName ?name ;
            vivo:relatedBy ?address .
        ?address a wos:Address ;
            vivo:relates ?pub, d:org-technical-university-of-denmark .
    }
    """
    for row in vstore.query(q):
        #print row.org, row.name
        existing_name = row.name
        pname = name_key.get(existing_name.toPython())
        if pname is not None:
            logger.info("Processing existing name {} to clean name {}.".format(existing_name, pname))
            addr_uris, to_remove = get_existing_address(row.org)
            rmg  += to_remove
            new_uri = slug_uri(pname, prefix="dtusuborg")
            addg.add((new_uri, RDF.type, WOS.SubOrganization))
            addg.add((new_uri, RDFS.label, Literal(pname)))
            addg.add((new_uri, WOS.subOrganizationName, Literal(pname)))
            addg.add((new_uri, WOS.subOrganizationNameVariant, existing_name))
            for auri in addr_uris:
                addg.add((auri, VIVO.relates, new_uri))

    return addg, rmg


def process(clean_file, dry=False):
    vstore = backend.get_store()

    name_key = dict()
    # Map clean names
    with open(clean_file) as inf:
        inf.next()
        for n, row in enumerate(csv.reader(inf)):
            pname, ename = row[0].strip(), row[1].strip()
            if ename == "":
                continue
            logger.info("Cleaning {} with variant {}.".format(pname, ename))
            # Always create variant for the preferred name too.
            name_key[pname] = pname
            name_key[ename] = pname

    addg, removeg = index_orgs(name_key)

    graphs = [ADDRESS_GRAPH, SUBORG_GRAPH]

    # Remove from these graphs
    for g in graphs:
        logger.info("Removing preferred name triples with {} triples from {} graph.".format(len(removeg), g))
        rm2 = vstore.bulk_remove(g, removeg)

    logger.info("Adding preferred name triples with {} triples.".format(len(addg)))
    add = vstore.bulk_add(, addg)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Preferred names')
    parser.add_argument('--dry-run', '-d', action="store_true", dest="dry", default=False, help="Dry run.")
    parser.add_argument('--suborg-names', '-s', required=True, type=str)
    args = parser.parse_args()
    done = process(args.suborg_names, dry=args.dry)
