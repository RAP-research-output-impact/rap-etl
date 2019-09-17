"""
Map data from the internal InCites API to VIVO.
"""
import argparse
import json
import os
import sys

from slugify import slugify
from rdflib import Graph, Literal, RDF, RDFS
from namespaces import rq_prefixes

from namespaces import (
    D,
    VIVO,
    WOS,
)

from lib import backend
from lib.utils import get_category_uri
from lib import utils
from publications import waan_uri

import settings


logger = settings.get_logger()

def local_name(uri):
    return uri.split('/')[-1]


def get_unified_orgs(release):
    out = []

    organizations_file = os.path.join("data", "rdf", '{:03d}'.format(release), "unified-orgs.nt")
    g = Graph().parse(organizations_file, format="nt")
    rsp = g.query(rq_prefixes + "SELECT ?uri ?name WHERE {?uri a wos:UnifiedOrganization; rdfs:label ?name}")

    for row in rsp:
        out.append((local_name(row.uri.toPython()), row.name.toPython()))

    logger.info("Found {} organizations.".format(len(out)))
    return out


def load_incites_json_file(release, name, ictype):
    fname = "org-" + slugify(name)
    try:
        with open('data/incites/{:03d}/{}/{}.json'.format(release, ictype, fname)) as inf:
            return json.load(inf)
    except IOError:
        logger.warn("Could not find metrics for {} (data/incites/{:03d}/{}/{}.json).".format(name, release, ictype, fname))
        return []


def save_rdf(release, graph, ng):
    name = local_name(ng)
    path = os.path.join(settings.RDF_PATH, '{:03d}'.format(release), name + '.nt')
    graph.serialize(destination=path, format='nt')
    return path


def org_total_counts(release, orgs):
    g = Graph()
    for org_name in orgs:
        org_uri = waan_uri(org_name)
        ln = local_name(org_uri)
        pcounts = load_incites_json_file(release, org_name, 'total')
        if len(pcounts) == 0:
            logger.warning("total/{} file is empty.".format("org-" + slugify(org_name)))
            continue
        for item in pcounts:
            curi = D['pubcount-' + ln + '-' + str(item['year'])]
            g.add((curi, RDF.type, WOS.InCitesPubPerYear))
            g.add((curi, RDFS.label, Literal("{} - {}".format(item['year'], item['count']))))
            g.add((curi, WOS.number, Literal(item['count'])))
            g.add((curi, WOS.year, Literal(int(item['year']))))
            g.add((org_uri, VIVO.relates, curi))
    return g


def org_total_cites(release, orgs):
    g = Graph()
    for org_name in orgs:
        org_uri = waan_uri(org_name)
        ln = local_name(org_uri)
        tc = load_incites_json_file(release, org_name, 'cites')
        if len(tc) == 0:
            logger.warning("cites/{} file is empty.".format("org-" + slugify(org_name)))
            continue
        for item in tc:
            curi = D['citecount-' + ln + '-' + str(item['year'])]
            g.add((curi, RDF.type, WOS.InCitesCitesPerYear))
            g.add((curi, RDFS.label, Literal("{} - {}".format(item['year'], item['count']))))
            g.add((curi, WOS.number, Literal(item['count'])))
            g.add((curi, WOS.year, Literal(int(item['year']))))
            g.add((org_uri, VIVO.relates, curi))

    return g


def org_top_categories(release, orgs):
    g = Graph()
    for org_name in orgs:
        org_uri = waan_uri(org_name)
        ln = local_name(org_uri)
        top_cat = load_incites_json_file(release, org_name, 'categories-by-year')
        if len(top_cat) == 0:
            logger.warning("categories-by-year/{} file is empty.".format("org-" + slugify(org_name)))
            continue
        for item in top_cat:
            cat = item['category']
            for tc_yr in item['counts']:
                count = tc_yr['count']
                year = tc_yr['year']
                category_uri = get_category_uri(cat)
                curi = D['topcategory-'] + ln + slugify(cat) + '-{}'.format(year)
                g.add((curi, RDF.type, WOS.InCitesTopCategory))
                g.add((curi, RDFS.label, Literal("{} - {}".format(org_name, cat))))
                g.add((curi, WOS.number, Literal(count)))
                g.add((curi, WOS.year, Literal(int(year))))
                g.add((curi, VIVO.relates, category_uri))
                g.add((curi, VIVO.relates, org_uri))
    return g


def main():
    """
    Get the orgs in the system and load the incites data for each.
    """
    parser = argparse.ArgumentParser(description='Map InCites data to RDF')
    parser.add_argument("--release", type=int)
    args = parser.parse_args()
    utils.release(args.release)
    if utils.RELEASE == 0:
        raise Exception("fatal: release not found: {}".format(args.release))
    to_load = []
    for ouri, name in get_unified_orgs(utils.RELEASE):
        to_load.append(name)

    top_cats = org_top_categories(utils.RELEASE, to_load)
    save_rdf(utils.RELEASE, top_cats, settings.INCITES_TOP_CATEGORIES)

    cites_by_year = org_total_cites(utils.RELEASE, to_load)
    save_rdf(utils.RELEASE, cites_by_year, settings.INCITES_TOTAL_CITES_YEAR)

    counts = org_total_counts(utils.RELEASE, to_load)
    save_rdf(utils.RELEASE, counts, settings.INCITES_PUB_YEAR_COUNTS)


if __name__ == "__main__":
    main()
