"""
Map WoS full record XML to an extended VIVO ontology.

This expects the XML to be on disk with one record per file.
Path to these files can be specified in settings.py.
"""

import argparse
import glob
import random
import sys
import xml.etree.ElementTree as ET


# installed
from rdflib import Graph, Literal, URIRef
from rdflib.resource import Resource
from slugify import slugify


# local
from lib import backend
from settings import (
    SEED,
    NS,
    RECORD_PATH,
    PUB_GRAPH,
    logger,
    DEPARTMENT_UNKNOWN_LABEL,
    COUNTRY_REPLACE,
    ADDED_COUNTRIES
)
from namespaces import (
    D,
    BIBO,
    WOS,
    RDF,
    RDFS,
    VIVO,
    XSD,
    OBO
)


def ln(uri):
    return uri.toPython().split('/')[-1]


def egf(elem, path):
    """Get element text by find-ing a path"""
    chunk = elem.find(path)
    if chunk is not None:
        txt = chunk.text
        if txt is not None:
            return txt.strip()


def slug_uri(text, prefix="e"):
    ln = slugify(unicode(text))
    return D[prefix + "-" + ln]


def waan_uri(text):
    ln = slugify(unicode(text))
    return D["org-" + ln]


def get_category_uri(name):
    return D['wosc-' + slugify(name)]


def add_category(value):
    """
    Don't change category label even though they aren't consistent.
    """
    g = Graph()
    uri = get_category_uri(value)
    g.add((uri, RDF.type, WOS.Category))
    g.add((uri, RDFS.label, Literal(value)))
    return uri, g


def add_keyword_plus(value, pub_uri):
    """
    Leave keywords plus as is.
    """
    g = Graph()
    slug = slugify(value)
    uri = D['kwp-' + slug]
    g.add((uri, RDF.type, WOS.KeywordPlus))
    g.add((uri, RDFS.label, Literal(value)))
    g.add((pub_uri, WOS.hasKeywordPlus, uri))
    return uri, g


def add_keyword_plus_data_property(value, pub_uri):
    """
    Leave keywords plus as is.
    """
    g = Graph()
    g.add((pub_uri, WOS.keywordPlus, Literal(value)))
    return g


def add_author_keyword(value, pub_uri):
    """
    Leave author keywords as is.
    """
    g = Graph()
    slug = slugify(value)
    uri = D['akw-' + slug]
    g.add((uri, RDF.type, WOS.AuthorKeyword))
    g.add((uri, RDFS.label, Literal(value)))
    g.add((pub_uri, WOS.hasAuthorKeyword, uri))
    return uri, g


def add_author_keyword_data_property(value, pub_uri):
    """
    Leave keywords plus as is.
    """
    g = Graph()
    g.add((pub_uri, WOS.authorKeyword, Literal(value)))
    return g


def add_grant(grant, pub_uri):
    """
    Create a funder and grant(s).
    """
    g = Graph()
    if grant.get("agency") is None:
        logger.info("No agency found for {} with ids.".format(pub_uri, ";".join(grant.get("ids", []))))
        return g
    slug = slugify(unicode(grant["agency"]))
    uri = D['funder-' + slug]
    g.add((uri, RDF.type, WOS.Funder))
    g.add((uri, RDFS.label, Literal(grant["agency"])))
    for gid in grant["ids"]:
        label = "{} - {}".format(grant["agency"], gid)
        guri = D['grant-'] + slugify(unicode(label))
        g.add((guri, RDF.type, WOS.Grant))
        g.add((guri, RDFS.label, Literal(label)))
        g.add((guri, WOS.grantId, Literal(gid)))
        g.add((guri, VIVO.relates, uri))
        g.add((guri, VIVO.relates, pub_uri))
    return g


class WosRecord(object):
    """
    Convert a XML string of WoS metadata to a Python object.
    """

    def __init__(self, xml_string):
        self.rec = ET.fromstring(xml_string)
        self.ut = self.rec.find('UID', NS).text
        self.summary = self.rec.find('static_data/summary', NS)
        self.full = self.rec.find('static_data/fullrecord_metadata', NS)
        self.dynamic = self.rec.find('dynamic_data', NS)
        self.item = self.rec.find('static_data/item', NS)
        self.pub_info = self.summary.find('pub_info', NS).attrib

    def _xfind(self, root, path):
        return root.find(path, NS)

    def summary(self):
        pass

    @staticmethod
    def _safe_get(elem, path):
        try:
            return elem.find(path, NS).text
        except AttributeError:
            return None

    def doc_type(self):
        """
        Get all doctypes
        """
        return [d.text for d in self.summary.findall('doctypes/doctype', NS)]

    def title(self):
        return self._safe_get(self.summary, 'titles/title[@type="item"]')

    def pub_date(self):
        return self.pub_info.get('sortdate')

    def pages(self):
        page = self.summary.find('pub_info/page', NS)
        if page is None:
            return None, None
        return page.attrib.get('begin'), page.attrib.get('end')

    def source(self):
        bid = self._safe_get(self.item, "bib_id")
        issn = self.get_id("issn")
        eissn = self.get_id("eissn")
        isbn = self.get_id("isbn")
        ptype = self.pub_info.get("pubtype")
        title = self._safe_get(self.summary, 'titles/title[@type="source"]')
        abbrv = self._safe_get(self.summary, 'titles/title[@type="source_abbrev"]')
        return dict(
            ut=self.ut,
            bid=bid,
            issn=issn,
            eissn=eissn,
            isbn=isbn,
            ptype=ptype,
            title=title,
            abbrv=abbrv
        )

    def keywords_plus(self):
        return [
            unicode(kp.text) for kp in self.item.findall('keywords_plus/keyword', NS)
        ]

    def categories(self):
        return [
            unicode(kp.text) for kp in self.full.findall('category_info/subjects/subject/[@ascatype="traditional"]')
        ]

    def author_keywords(self):
        return [
            unicode(kp.text) for kp in self.full.findall('keywords/keyword', NS)
        ]

    def abstract(self):
        abe = self.full.find('abstracts/abstract/abstract_text', NS)
        if abe is not None:
            raw = u" ".join([a.text for a in abe.findall('p')])
            return raw

    def get_id(self, id_type):
        elem = self.dynamic.find(
            'cluster_related/identifiers/identifier[@type="' + id_type + '"]',
            NS
        )
        if elem is not None:
            return elem.attrib.get('value')
        return None

    def author_list(self):
        out = []
        for au in self.summary.findall('names/name'):
            fn = au.find('./full_name')
            if fn is not None:
                out.append(fn.text)

        return ", ".join(out)

    def authors(self):
        out = []
        for au in self.summary.findall('names/name'):
            out.append(
                dict(
                    rank=au.attrib['seq_no'],
                    dais_ng=au.attrib.get('daisng_id'),
                    address=au.attrib.get('addr_no'),
                    reprint=au.attrib.get("reprint"),
                    email=egf(au, 'email_addr'),
                    display_name=au.find('display_name').text,
                    full_name=egf(au, 'full_name'),
                    wos_standard=egf(au, 'wos_standard'),
                    first=egf(au, 'first_name'),
                    last=egf(au, 'last_name')
                )
            )
        return out

    def addresses(self):
        out = []
        for addr in self.full.findall('addresses/address_name', NS):
            spec = addr.find('address_spec')
            no = spec.attrib['addr_no']
            full_address = spec.find('full_address').text
            orgs = []
            unified_orgs = []
            for org in spec.findall('organizations/organization'):
                name = org.text
                if org.attrib.get('pref', 'N') == "Y":
                    unified_orgs.append(name)
                else:
                    orgs.append(name)
            if len(orgs) > 1:
                raise Exception("Multiple organizations/organization found for {}".format(self.ut))
            sub_orgs = []
            for org in spec.findall('suborganizations/suborganization'):
                name = org.text
                sub_orgs.append(name)
            try:
                org_name = orgs[0]
            except IndexError:
                org_name = "n/a"
            if sub_orgs == []:
                sub_orgs = [DEPARTMENT_UNKNOWN_LABEL]
            country = spec.find('country').text
            out.append(
                dict(
                    number=no,
                    full_address=full_address,
                    organization=org_name,
                    sub_organizations=sub_orgs,
                    unified_orgs=unified_orgs,
                    country=country
                )
            )
        return out

    def funding_acknowledgement(self):
        abe = self.full.find('fund_ack/fund_text', NS)
        if abe is not None:
            raw = u" ".join([a.text for a in abe.findall('p')])
            return raw

    def grants(self):
        out = []
        gs = self.full.find('fund_ack/grants', NS)
        if gs is not None:
            for grant in gs.findall('grant', NS):
                d = {}
                d['agency'] = self._safe_get(grant, 'grant_agency')
                d['ids'] = [g.text for g in grant.findall('grant_ids/grant_id')]
                out.append(d)
        return out

    def reference_count(self):
        refs = self.full.find('refs', NS)
        return int(refs.attrib['count'])

    def citation_count(self):
        cites = self.dynamic.find('citation_related/tc_list/silo_tc', NS)
        return int(cites.attrib['local_count'])

    def meta(self):
        return {
            'ut': self.ut,
            'doc_type': self.doc_type(),
            'title': self.title(),
            'pub_date': self.pub_date(),
            'source': self.source(),
            'cite_key': self._safe_get(self.rec.find('static_data'), 'item/bib_id'),
            'author_list': self.author_list(),
            'authors': self.authors(),
            'addresses': self.addresses(),
            'doi': self.get_id("doi"),
            'issue': self.pub_info.get('issue'),
            'volume': self.pub_info.get('vol'),
            'issue': self.pub_info.get('issue'),
            'start': self.pages()[0],
            'end': self.pages()[1],
            'author_keywords': self.author_keywords(),
            'keywords_plus': self.keywords_plus(),
            'categories': self.categories(),
            'abstract': self.abstract(),
            'funding_acknowledgement': self.funding_acknowledgement(),
            'grants': self.grants(),
            'citation_count': self.citation_count(),
            'reference_count': self.reference_count()
        }


class RDFRecord(WosRecord):

    """
    Represent the WoS Record as RDF.
    """

    @property
    def uri(self):
        return D[self.ln]

    @property
    def ln(self):
        return "pub-" + self.ut.replace(':', '')

    def aship_uri(self, no):
        return D["au" + no + "-" + self.ln]

    def addr_uri(self, raw, num):
        if raw is None:
            raise Exception("No full address to create URI")
        ln = backend.hash_local_name("addr", raw) + num
        return D[ln]

    def addr_uris_from_number(self, number):
        out = []
        for addr in self.addresses():
            num = addr['number']
            if num == number:
                faddr = addr["full_address"]
                out.append(self.addr_uri(faddr, num))
        return out

    @staticmethod
    def sub_org_uri(label):
        ln = backend.hash_local_name("suborg", label)
        return D[ln]

    def rec_type(self):
        """
        WOS record types
        http://images.webofknowledge.com/WOKRS59B4/help/WOS/hs_document_type.html
        :return: list
        """
        d = {
            "Article": WOS.Article,
            "Abstract of Published Item": WOS.Abstract,
            "Art Exhibit Review": WOS.ArtExhibitReview,
            "Biographical-Item": WOS.BiographicalItem,
            "Book": BIBO.Book,
            "Book Chapter": WOS.BookChapter,
            "Book Review": WOS.BookReview,
            "Chronology": WOS.Chronology,
            "Correction": WOS.Correction,
            "Correction, Addition": WOS.CorrectionEdition,
            "Dance Performance Review": WOS.DancePerformanceReview,
            "Database Review": WOS.DatabaseReview,
            "Discussion": WOS.Discussion,
            "Editorial Material": WOS.EditorialMaterial,
            "Excerpt": WOS.Excerpt,
            "Fiction, Creative Prose": WOS.FictionCreativeProse,
            "Film Review": WOS.FilmReview,
            "Hardware Review": WOS.HardwareReview,
            "Item About An Individual": WOS.ItemAboutAnIndividual,
            "Letter": WOS.Letter,
            "Meeting Abstract": WOS.MeetingAbstract,
            "Meeting Summary": WOS.MeetingSummary,
            "Music Performance Review": WOS.MusicPerformanceReview,
            "Music Score": WOS.MusicScore,
            "Music Score Review": WOS.MusicScoreReview,
            "News Item": WOS.NewsItem,
            "Note": WOS.Note,
            "Poetry": WOS.Poetry,
            "Proceedings Paper": WOS.ProceedingsPaper,
            "Record Review": WOS.RecordReview,
            "Reprint": WOS.Reprint,
            "Review": WOS.Review,
            "Script": WOS.Script,
            "Software Review": WOS.SoftwareReview,
            "TV Review, Radio Review": WOS.TVRadioReview,
            "TV Review, Radio Review, Video Review": WOS.TVRadioVideoReview,
            "Theater Review": WOS.TheaterReview
        }
        dtypes = []
        for wdt in self.doc_type():
            vtype = d.get(wdt)
            if vtype is None:
                logger.info("Publication type unknown for {}, {}.".format(self.ut, wdt))
            else:
                dtypes.append(vtype)
        if dtypes == []:
            return [WOS.Publication]
        return dtypes

    def authorships(self):
        g = Graph()
        aus = self.authors()
        for au in aus:
            aship_uri = self.aship_uri(au['rank'])
            r = Resource(g, aship_uri)
            r.set(RDFS.label, Literal(au["display_name"]))
            r.set(RDF.type, VIVO.Authorship)
            r.set(VIVO.rank, Literal(au['rank']))
            data_props = [
                ('rank', VIVO.rank),
                ('full_name', WOS.fullName),
                ('display_name', WOS.displayName),
                ('wos_standard', WOS.standardName),
                ('first', WOS.firstName),
                ('last', WOS.lastName),
                ('email', WOS.email),
                ('dais_ng', WOS.daisNg),
                ('reprint', WOS.reprint),
            ]
            for key, prop in data_props:
                value = au.get(key)
                if value is not None:
                    r.set(prop, Literal(value))
            # relations
            r.add(VIVO.relates, self.uri)
            # relate to addresses too
            # address nums are a space separated list of numbers
            addr_nums = au["address"]
            if addr_nums is None:
                continue
            else:
                for anum in addr_nums.split():
                    addr_uris = self.addr_uris_from_number(anum)
                    for auri in addr_uris:
                        r.add(VIVO.relates, auri)
        return g

    def sub_orgs(self):
        g = Graph()
        addresses = self.addresses()
        for addr in addresses:
            ano = addr["number"]
            org = addr["organization"]
            for idx, suborg in enumerate(addr['sub_organizations']):
                label = "{}, {}".format(suborg, org)
                uri = self.sub_org_uri(label)
                r = Resource(g, uri)
                r.set(RDF.type, WOS.SubOrganization)
                r.set(RDFS.label, Literal(label))
                r.set(WOS.organizationName, Literal(org));
                r.set(WOS.subOrganizationName, Literal(suborg))
        return g

    def _country(self, name):
        added_uri = ADDED_COUNTRIES.get(name)
        if added_uri is not None:
            return added_uri
        else:
            short_name = COUNTRY_REPLACE.get(name)
            if short_name is None:
                short_name = name.replace(' ', '_')
            uri = URIRef('http://aims.fao.org/aos/geopolitical.owl#' + short_name)
            return uri

    def unified_orgs(self):
        g = Graph()
        addresses = self.addresses()
        for addr in addresses:
            for org in addr["unified_orgs"]:
                uri = waan_uri(org)
                r = Resource(g, uri)
                r.set(RDF.type, WOS.UnifiedOrganization)
                r.set(RDFS.label, Literal(org))
                country_uri = self._country(addr["country"])
                r.set(OBO['RO_0001025'], country_uri)
                # relation set by address
        return g

    def addressships(self):
        g = Graph()
        addresses = self.addresses()
        for addr in addresses:
            addr_uri = self.addr_uri(addr["full_address"], addr["number"])
            org = addr["organization"]
            r = Resource(g, addr_uri)
            r.set(RDF.type, WOS.Address)
            r.set(RDFS.label, Literal(addr['full_address']))
            r.set(WOS.organizationName, Literal(org))
            r.set(WOS.sequenceNumber, Literal(addr['number']))
            # relation to author set by authorship
            # relate to pub
            r.set(VIVO.relates, self.uri)
            # sub orgs
            for idx, suborg in enumerate(addr["sub_organizations"]):
                label = "{}, {}".format(suborg, org)
                so_uri = self.sub_org_uri(label)
                r.add(VIVO.relates, so_uri)
            # relate unified orgs
            for uorg in addr["unified_orgs"]:
                uo_uri = waan_uri(uorg)
                r.add(VIVO.relates, uo_uri)
        return g

    def categories_g(self):
        g = Graph()
        for cat in self.categories():
            cat_uri = get_category_uri(cat)
            g.add((cat_uri, RDF.type, WOS.Category))
            g.add((cat_uri, RDFS.label, Literal(cat)))
            g.add((self.uri, WOS.hasCategory, cat_uri))
        return g

    @staticmethod
    def make_date_uri(pub_id, year):
        part = backend.hash_local_name("date", pub_id + year)
        return D[part]

    def add_pub_date(self):
        """
        Publication dates in VIVO's expected format.
        """
        g = Graph()
        value = self.pub_date()
        if value is None:
            return g
        date_uri = self.make_date_uri(self.ut, value)
        date = Resource(g, date_uri)
        date.set(RDF.type, VIVO.DateTimeValue)
        date.set(VIVO.dateTimePrecision, VIVO.yearMonthDayPrecision)
        date.add(
            VIVO.dateTime,
            Literal("%sT00:00:00" % (value), datatype=XSD.dateTime)
        )
        date.add(RDFS.label, Literal(value))
        date.set(RDF.type, VIVO.DateTimeValue)
        date.set(VIVO.dateTimePrecision, VIVO.yearMonthDayPrecision)
        date.add(
            VIVO.dateTime,
            Literal("%sT00:00:00" % (value), datatype=XSD.dateTime)
        )
        date.add(RDFS.label, Literal(value))
        g.add((self.uri, VIVO.dateTimeValue, date_uri))
        return g

    def venue(self):
        g = Graph()
        source = self.source()
        # Make uri - hash of source key less unique attributes
        d = source
        d.pop('bid')
        d.pop('ut')
        uri = D['venue' + str(abs(hash(frozenset(d.items()))))]

        source_type = source['ptype']
        doc_types = self.doc_type()
        if source_type == 'Journal':
            vtype = BIBO.Journal
        elif (source_type == "Book") and ("Proceedings Paper" in doc_types):
            vtype = WOS.Conference
        elif source_type in ('Book', 'Books', 'Book in series', 'Books in series'):
            vtype = BIBO.Book
        else:
            raise Exception("Unknown venue type")
        g.add((uri, RDF.type, vtype))
        g.add((uri, RDFS.label, Literal(source['title'])))
        if source.get('abbrv') is not None:
            g.add((uri, WOS.journalAbbr, Literal(source['abbrv'])))

        props = [
            ('issn', BIBO.issn),
            ('eissn', BIBO.eissn),
            ('isbn', BIBO.isbn)
        ]
        for k, prop in props:
            val = source.get(k)
            if val is not None:
                g.add((uri, prop, Literal(val)))

        # pub relationship
        g.add((self.uri, VIVO.hasPublicationVenue, uri))
        return g

    def to(self):
        """
        Core publication metadata mapped to VIVO RDF.
        :return: Graph
        """
        g = Graph()
        r = Resource(g, self.uri)
        r.set(RDFS.label, Literal(self.title()))
        for vtype in self.rec_type():
            r.add(RDF.type, vtype)
        r.set(WOS.wosId, Literal(self.ut))

        meta = self.meta()
        # data properties
        data_props = [
            #('author_list', WOS.authorList),
            ('abstract', BIBO.abstract),
            ('funding_acknowledgement', WOS.fundingText),
            ('volume', BIBO.volume),
            ('issue', BIBO.issue),
            ('start', BIBO.pageStart),
            ('end', BIBO.pageEnd),
            ('page_count', BIBO.numPages),
            ('doi', BIBO.doi),
            #('cite_key', WOS.citeKey),
            ('reference_count', WOS.referenceCount),
            ('citation_count', WOS.citationCount)
        ]
        for key, prop in data_props:
            value = meta.get(key)
            if value is not None:
                g.add((self.uri, prop, Literal(value)))

        g += self.add_pub_date()

        return g


def file_path_to_meta(name):
    """
    Take a path to WOS XML doc and convert to record object.
    """
    with open(name) as inf:
        raw = inf.read()
        return RDFRecord(raw)


def get_data_files():
    return [df for df in glob.glob(RECORD_PATH)]


def sample_data_files(num):
    random.seed(SEED)
    data_files = get_data_files()
    to_load = [data_files[n] for n in random.sample(xrange(len(data_files)), num)]
    return to_load


def load_pubs_set(to_load):
    g = Graph()
    for name in to_load:
        rec = file_path_to_meta(name)
        meta = rec.meta()
        logger.info("Mapping {} to RDF.".format(meta['ut']))
        g += rec.to()

    logger.info("Loading publications")
    # Update the graph
    backend.post_updates(PUB_GRAPH, g)


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sample", type=int, help="Number of sample publications to index")
    parser.add_argument("-a", "--all", action="store_true", help="Index all publications" )
    return parser.parse_args(args)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args is None:
        raise Exception("No arguments passed.")
    elif args.sample is not None:
        logger.info("Sampling {} publications for load.".format(args.sample))
        sample = sample_data_files(args.sample)
        load_pubs_set(sample)
    elif args.all is True:
        logger.info("Loading all publications")
        data_files = get_data_files()
        load_pubs_set(data_files)
    else:
        raise Exception("Options not understood.")


