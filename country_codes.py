"""
Map org enhanced to 3 digit country codes using address info from
Web of Science org enhanced file. Also map additional desired attributes.
"""

import argparse
import sys
import csv
import json

from slugify import slugify

from publications import waan_uri

from lib import backend

from namespaces import WOS, rq_prefixes, VIVO, OBO
from rdflib import Graph, Literal, URIRef

from log_setup import get_logger

logger = get_logger()

from settings import COUNTRY_CODE_NG, COUNTRY_CODE_KEY_FILE


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
    return d


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process org enhanced')
    parser.add_argument('--index', '-i', default=None, help="Index the orgs from raw WOS metadata list. Pass metadata file.")
    parser.add_argument('--load', '-l', action="store_true", help="Load updated country codes into VIVO.")
    args = parser.parse_args()

    if (args.index is None) and (args.load is not True):
        print>>sys.stderr, "No action specified"
        sys.exit()

    COUNTRY_CODE_KEY_FILE = "data/org_key.json"

    logger.info("Fetching VIVO countries")
    vcountries = fetch_vivo_countries()

    if args.index is True:
        org_enhanced_meta = sys.argv[1]
        orgs = index_org_metadata(org_enhanced_meta, vcountries)
        with open(COUNTRY_CODE_KEY_FILE, 'wb') as outf:
            json.dump(orgs, outf)

    if args.load is True:
        with open(COUNTRY_CODE_KEY_FILE) as inf:
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

        backend.post_updates(COUNTRY_CODE_NG, g)
