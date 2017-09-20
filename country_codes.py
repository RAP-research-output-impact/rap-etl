"""
Map org enhanced to 3 digit country codes using address info from
Web of Science org enhanced file. Also map additional desired attributes.
"""


import sys
import csv
import json

from slugify import slugify

from map_pubs import waan_uri

import backend

from namespaces import WOS, rq_prefixes, VIVO, OBO
from rdflib import Graph, Literal, URIRef

from log_setup import get_logger

logger = get_logger()

NAMED_GRAPH = "http://localhost/data/organization-extra"


REPL = {
    'UNITED STATES': 'united-states-of-america',
    'PEOPLES R CHINA': 'china',
    'RUSSIA': 'russian-federation',
    'VENEZUELA': 'venezuela-bolivarian-republic-of',
    'BOLIVIA': 'bolivia',
    'CZECH REPUBLIC': 'czech-republic',
    'IVORY COAST': 'ivory-cost',
    'TAIWAN': 'china',
    'SOUTH KOREA': 'south-korea',
    'VIETNAM': 'viet-nam',
    'IRAN': 'iran-islamic-republic-of',
    'REPUBLIC OF GEORGIA': 'georgia',
    'REUNION': 'reunion',
    'TANZANIA': 'united-republic-of-tanzania',
    'BOSNIA & HERZEGOVINA': 'bosnia-herzegovina',
}


store = backend.get_store()


def mk_slug(raw):
    clean = raw.strip().lower()
    return slugify(clean)


def process_country_codes(fpath):
    d = {}
    with open(fpath) as raw:
        for row in csv.DictReader(raw):
            name = row['ISO4217-currency_country_name']
            if name == "":
                continue
            code = row['ISO3166-1-Alpha-3']
            slug = country_slug(name)
            d[slug] = code
    return d


def process_org_enhanced(fpath, country_codes):
    g = Graph()
    with open(fpath) as raw:
        for row in csv.reader(raw, delimiter="\t"):
            matched = None
            waan_id = row[0]
            name = row[1]
            country = row[6].split(',')[-1]
            clean_country = country.strip().upper()
            url = row[9]
            #print name, country
            try:
                uri = waan_uri(name)
            except UnicodeDecodeError:
                print>>sys.stderr, row
            cslug = country_slug(country)
            matched = REPL.get(clean_country)
            if matched is None:
                matched = country_codes.get(cslug)

            if matched is None:
                print>>sys.stderr, clean_country, cslug, "***** no match"
                continue
            if matched == 'CZC':
                import ipdb; ipdb.set_trace()
                print>>sys.stderr, "False match", clean_country, matched

            g.add((uri, WOS.countryCode, Literal(matched)))
            g.add((uri, WOS.waanId, Literal(waan_id)))
            if url != "":
                g.add((uri, WOS.url, Literal(url)))
    backend.post_updates(NAMED_GRAPH, g)


def load():
    argv = sys.argv[1]
    country_codes = process_country_codes(argv)
    org_enhanced_meta = sys.argv[2]
    process_org_enhanced(org_enhanced_meta, country_codes)


def fetch_vivo_countries():
    q = rq_prefixes + """
    SELECT ?uri ?label ?code
    WHERE {
      ?uri a vivo:Country ;
           rdfs:label ?label ;
           <http://aims.fao.org/aos/geopolitical.owl#codeISO3> ?code .
    }
    """
    d = {}
    for row in store.query(q):
        slug = mk_slug(row.label.toPython())
        d[slug] = row.uri
    return d


def fetch_vivo_states():
    q = rq_prefixes + """
    SELECT ?uri ?label
    WHERE {
      ?uri a vivo:StateOrProvince ;
           rdfs:label ?label .
    }
    """
    d = {}
    for row in store.query(q):
        slug = mk_slug(row.label.toPython())
        d[slug] = row.uri
    return d


def get_orgs():
    q = rq_prefixes + """
    SELECT ?uri ?label
    WHERE {
      ?uri a wos:UnifiedOrganization ;
           rdfs:label ?label .
    }
    """
    out = []
    for row in store.query(q):
        out.append(row.uri.toPython())
    return out


def index_org_metadata(fpath, to_match):
    d = {}
    with open(fpath) as raw:
        for row in csv.reader(raw, delimiter="\t"):
            td = {}
            waan_id = row[0]
            name = row[1]
            uri = waan_uri(name)
            country = row[6].split(',')[-1]
            clean_country = country.strip().upper()
            url = row[9]
            d[uri] = dict(
                waan_id=waan_id,
                name=row[1],
                country=clean_country,
            )
    with open('data/org_enhanced/org_key.json', 'wb') as outf:
        json.dump(d, outf)
    return True


if __name__ == "__main__":
    vcountries = fetch_vivo_countries()
    vstates = fetch_vivo_states()

    org_enhanced_meta = sys.argv[1]
    index_org_metadata(org_enhanced_meta, vcountries)
    with open('data/org_enhanced/org_key.json') as inf:
        org_enhanced = json.load(inf)

    orgs = get_orgs()

    g = Graph()
    for org in orgs:
        ometa = org_enhanced.get(org)
        if ometa is None:
            logger.info("No org metadata found for {}.".format(org))
            continue
        country = ometa['country']
        name = REPL.get(country) or mk_slug(country)
        try:
            curi = vcountries[name]
            g.add((URIRef(org), OBO['RO_0001025'], URIRef(curi)))
        except:
            logger.info("Can't match {}.".format(name))

    backend.post_updates(NAMED_GRAPH, g)
