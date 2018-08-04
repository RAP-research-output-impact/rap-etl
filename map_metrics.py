"""
Map data from the internal InCites API to VIVO.
"""
import json
import os

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
from publications import waan_uri

import settings


logger = settings.get_logger()

RELEASE = sys.argv[1]


def local_name(uri):
    return uri.split('/')[-1]


def get_unified_orgs():
    q = rq_prefixes + """
    select ?wosU ?org
    where {
        ?wosU a wos:UnifiedOrganization ;
            rdfs:label ?org .
    }
    """
    vstore = backend.get_store()
    out = []
    for row in vstore.query(q):
        out.append((row.wosU.toPython(), row.org.toPython()))
    return out


def load_incites_json_file(name, ictype):
    fname = "org-" + slugify(name)
    try:
        with open('data/incites/v{}/{}/{}.json'.format(RELEASE, ictype, fname)) as inf:
            return json.load(inf)
    except IOError:
        logger.warn("Could not find metrics for {}.".format(name))
        return []


def save_rdf(graph, ng):
    name = local_name(ng)
    path = os.path.join(settings.RDF_PATH, name + '.nt')
    graph.serialize(destination=path, format='nt')
    return path


def org_total_counts(orgs):
    g = Graph()
    for org_name in orgs:
        org_uri = waan_uri(org_name)
        ln = local_name(org_uri)
        pcounts = load_incites_json_file(org_name, 'total')
        if len(pcounts) == 0:
            logger.warning("{} file is empty.".format(org_name))
            continue
        for item in pcounts:
            curi = D['pubcount-' + ln + '-' + str(item['year'])]
            g.add((curi, RDF.type, WOS.InCitesPubPerYear))
            g.add((curi, RDFS.label, Literal("{} - {}".format(item['year'], item['count']))))
            g.add((curi, WOS.number, Literal(item['count'])))
            g.add((curi, WOS.year, Literal(item['year'])))
            g.add((org_uri, VIVO.relates, curi))
    return g


def org_total_cites(orgs):
    g = Graph()
    for org_name in orgs:
        org_uri = waan_uri(org_name)
        ln = local_name(org_uri)
        tc = load_incites_json_file(org_name, 'cites')
        if len(tc) == 0:
            logger.warning("{} file is empty.".format(org_name))
            continue
        for item in tc:
            curi = D['citecount-' + ln + '-' + str(item['year'])]
            g.add((curi, RDF.type, WOS.InCitesCitesPerYear))
            g.add((curi, RDFS.label, Literal("{} - {}".format(item['year'], item['count']))))
            g.add((curi, WOS.number, Literal(item['count'])))
            g.add((curi, WOS.year, Literal(item['year'])))
            g.add((org_uri, VIVO.relates, curi))

    return g


def org_top_categories(orgs):
    g = Graph()
    for org_name in orgs:
        org_uri = waan_uri(org_name)
        ln = local_name(org_uri)
        top_cat = load_incites_json_file(org_name, 'categories-by-year')
        if len(top_cat) == 0:
            logger.warning("{} file is empty.".format(org_name))
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
                g.add((curi, WOS.year, Literal(year)))
                g.add((curi, VIVO.relates, category_uri))
                g.add((curi, VIVO.relates, org_uri))
    return g


def main():
    """
    Get the orgs in the system and load the incites data for each.
    """
    to_load = []
    for ouri, name in get_unified_orgs():
        to_load.append(name)

    top_cats = org_top_categories(to_load)
    save_rdf(top_cats, settings.INCITES_TOP_CATEGORIES)

    cites_by_year = org_total_cites(to_load)
    save_rdf(cites_by_year, settings.INCITES_TOTAL_CITES_YEAR)

    counts = org_total_counts(to_load)
    save_rdf(counts, settings.INCITES_PUB_YEAR_COUNTS)


if __name__ == "__main__":
    main()
