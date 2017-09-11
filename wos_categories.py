"""
Relate publication venues to categories using WOS category file.
"""

import csv
import os
import sys

from slugify import slugify

from rdflib import Graph, Literal
from rdflib.namespace import RDF, RDFS

from collections import defaultdict

from lib import backend

from namespaces import WOS, D, rq_prefixes

from settings import CATEGORY_NG, CATEGORY_FILE


def get_category_uri(name):
    return D['wosc-' + slugify(name)]


def get_journals():
    q = rq_prefixes + """

    select ?j ?issn
    where {
        ?j bibo:issn ?issn .
    }
    """
    vstore = backend.get_store()
    d = {}
    for row in vstore.query(q):
        d[row.issn.toPython()] = row.j
    return d


def read_categories(input_file):
    d = defaultdict(list)
    with open(input_file) as inf:
        for row in csv.DictReader(inf):
            cat = row['WoS Category'].strip("\"").strip()
            d[row['ISSN'].strip()].append(unicode(cat))
    return d


def add_category(value):
    """
    Upper case all categories for now. They aren't consistent in the data.
    """
    g = Graph()
    uri = get_category_uri(value)
    g.add((uri, RDF.type, WOS.Category))
    g.add((uri, RDFS.label, Literal(value.upper())))
    return uri, g


def map_journals_to_categories(jrn_key, cat_key):
    g = Graph()
    for issn, juri in jrn_key.items():
        for cat in cat_key.get(issn, []):
            curi = get_category_uri(cat)
            #g += cg
            g.add((juri, WOS.hasCategory, curi))
    return g


def map_categories():
    jrn_key = get_journals()
    cat_key = read_categories(CATEGORY_FILE)
    g = Graph()
    # jrnls to categories
    g += map_journals_to_categories(jrn_key, cat_key)
    added, removed = backend.sync_updates(CATEGORY_NG, g)
    return added, removed

if __name__ == "__main__":
    map_categories()
