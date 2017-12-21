"""
Map data from the internal InCites API to VIVO.
"""
import glob
import sys
import json

from slugify import slugify

from rdflib import Graph, Literal, RDF, RDFS, URIRef
from rdflib.resource import Resource

from namespaces import rq_prefixes

from namespaces import (
	D,
	VIVO,
	OBO,
	VCARD,
	WOS,
	FOAF
)

from lib import backend
vstore = backend.get_store()

from wos_categories import get_category_uri
from publications import waan_uri

from log_setup import get_logger

logger = get_logger()

def hash_uri(prefix, value):
    return D[prefix + '-' + hashlib.md5(value).hexdigest()]


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
    out = []
    for row in vstore.query(q):
        out.append((row.wosU.toPython(), row.org.toPython()))
    return out

def load_incites_json_file(name, ictype):
    fname = slugify(name)
    try:
        with open('data/incites/{}/{}.json'.format(ictype, fname)) as inf:
            return json.load(inf)
    except IOError:
        logger.warn("Could not find metrics for {}.".format(name))
        return []

def org_total_counts(orgs):
    #pcounts = incites_api.get_total_pubs(name)
    g = Graph()
    for org_name in orgs:
        org_uri = waan_uri(org_name)
        ln = local_name(org_uri)
        pcounts = load_incites_json_file(org_name, 'total')
        for item in pcounts:
            curi = D['pubcount-' + ln + '-' + str(item['year'])]
            g.add((curi, RDF.type, WOS.InCitesPubPerYear))
            g.add((curi, RDFS.label, Literal("{} - {}".format(item['year'], item['count']))))
            g.add((curi, WOS.number, Literal(item['count'])))
            g.add((curi, WOS.year, Literal(item['year'])))
            g.add((org_uri, VIVO.relates, curi))
    ng = "http://localhost/data/incites-pub-year-counts"
    backend.sync_updates(ng, g)
    return True


def org_total_cites(orgs):
    g = Graph()
    for org_name in orgs:
        org_uri = waan_uri(org_name)
        #print>>sys.stderr, "Processing", org_name, "total cites"
        ln = local_name(org_uri)
        tc = load_incites_json_file(org_name, 'cites')
        for item in tc:
            curi = D['citecount-' + ln + '-' + str(item['year'])]
            g.add((curi, RDF.type, WOS.InCitesCitesPerYear))
            g.add((curi, RDFS.label, Literal("{} - {}".format(item['year'], item['count']))))
            g.add((curi, WOS.number, Literal(item['count'])))
            g.add((curi, WOS.year, Literal(item['year'])))
            g.add((org_uri, VIVO.relates, curi))


    #print g.serialize(format="turtle")
    ng = "http://localhost/data/incites-total-cites-year-counts"
    backend.sync_updates(ng, g)
    return True


def org_top_categories(orgs):
    g = Graph()
    for org_name in orgs:
        #print>>sys.stderr, "Processing", org_name, "top categories"
        org_uri = waan_uri(org_name)
        ln = local_name(org_uri)
        top_cat = load_incites_json_file(org_name, 'categories')
        for item in top_cat:
            cat = item['category']
            category_uri = get_category_uri(cat)
            curi = D['topcategory-'] + ln + slugify(cat)
            g.add((curi, RDF.type, WOS.InCitesTopCategory))
            g.add((curi, RDFS.label, Literal("{} - {}".format(org_name, cat))))
            g.add((curi, WOS.number, Literal(item['count'])))
            g.add((curi, VIVO.relates, category_uri))
            g.add((curi, VIVO.relates, org_uri))
    #print g.serialize(format="turtle")
    ng = "http://localhost/data/incites-top-categories"
    backend.sync_updates(ng, g)
    return True


def get_orgs():
    with open('data/collab_orgs.json') as inf:
        orgs = json.load(inf)
        for name, v in orgs.items():
            yield name

def main():
    """
    Get the orgs in the system and load the incites data for each.
    """
    to_load = []
    for ouri, name in get_unified_orgs():
        to_load.append(name)

    org_top_categories(to_load)
    org_total_cites(to_load)
    org_total_counts(to_load)



if __name__ == "__main__":
    main()