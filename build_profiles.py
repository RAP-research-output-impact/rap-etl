"""
Build person profiles using ORCID, ResearcherID, email data from WOS records.

"""

from collections import defaultdict
import json

from rdflib import Graph, URIRef, Literal
from rdflib.query import ResultException
from rdflib.resource import Resource

from lib import backend
import publications
from namespaces import (
    rq_prefixes,
    WOS,
    VIVO,
    FOAF,
    RDF,
    RDFS,
    D,
    VCARD,
    OBO
)

from settings import (
    logger,
    NS,
    AFFILIATION_NG,
    ORCID_FILE,
    RID_FILE,
    AU_ID_FILE
)


class Researcher(object):

    def __init__(self, profile):
        self.profile = profile
        # Use dais ids as URI key. If multiple
        # exist, sort and take the first one.
        dais_ids = profile['dais']
        if type(dais_ids) == list:
            dais_ids.sort()
            self.vid = dais_ids[0]
        else:
            dids = [d for d in dais_ids.split("|")]
            dids.sort()
            self.vid = dids[0]
        self.uri = D["person-" + self.vid]

    @property
    def vcard_uri(self):
        return D["vci" + self.vid]

    @property
    def vcard_name_uri(self):
        return D["vcn" + self.vid]

    @property
    def vcard_title_uri(self):
        return D["vct" + self.vid]

    @property
    def vcard_email_uri(self):
        return D["vce" + self.vid]

    @property
    def vcard_phone_uri(self):
        return D["vcp" + self.vid]

    def to_rdf(self):
        data = self.profile
        g = Graph()
        person = Resource(g, self.uri)
        person.set(RDF.type, FOAF.Person)
        person.set(RDFS.label, Literal(data['name'].strip()))
        # For sorting
        person.set(FOAF.familyName, Literal(data['lastName']))
        person.set(WOS.alphaBrowse, Literal(data['lastName'][0].lower()))

        # dais
        if type(data['dais']) == str:
            dais_ids = [data['dais']]
        else:
            dais_ids = data['dais']
        for did in dais_ids:
            person.set(WOS.daisNg, Literal(did))

        # Vcard individual
        vci_uri =self.vcard_uri
        person.set(OBO['ARG_2000028'], vci_uri)
        g.add((vci_uri, RDF.type, VCARD.Individual))

        # Vcard Name
        g += self._vcard_name()
        g.add((vci_uri, VCARD.hasName, URIRef(self.vcard_name_uri)))

        # Vcard email
        vte = self._vcard_email()
        if vte is not None:
            g += vte
            g.add((vci_uri, VCARD.hasEmail, URIRef(self.vcard_email_uri)))

        return g

    def _vcard_name(self):
        g = Graph()
        vc = Resource(g, URIRef(self.vcard_name_uri))
        vc.set(RDF.type, VCARD.Name)
        vc.set(RDFS.label, Literal(self.profile['name'].strip()))
        vc.set(VCARD.familyName, Literal(self.profile['lastName']))
        vc.set(VCARD.givenName, Literal(self.profile['firstName']))
        return g

    def _vcard_title(self):
        title = self.profile['position_title']
        g = Graph()
        vt = Resource(g, self.vcard_title_uri)
        vt.set(RDF.type, VCARD.Title)
        vt.set(RDFS.label, Literal(title))
        vt.set(VCARD.title, Literal(title))
        return g

    def _vcard_email(self):
        g = Graph()
        try:
            emails = [e for e in self.profile["emails"].split("|")]
        except KeyError:
            emails = [self.profile['email']]
        for email in emails:
            vt = Resource(g, self.vcard_email_uri)
            vt.set(RDF.type, VCARD.Work)
            # Label probably not necessary
            vt.set(RDFS.label, Literal(email))
            vt.set(VCARD.email, Literal(email))
        return g


def index_contributors(pub):
    au_idx = defaultdict(list)
    exists = False
    for contrib in pub.rec.find('static_data/contributors/contributor', NS) or []:
        last = contrib.find('last_name').text
        first = contrib.find('first_name').text
        orcid = contrib.attrib.get('orcid_id')
        r_id = contrib.attrib.get('r_id')
        au_idx[last].append(dict(last=last, first=first, orcid=orcid, r_id=r_id))
        exists = True
    if exists is False:
        return
    return au_idx


def build_orcid_rid_profiles():
    """
    Builds profiles for researchers with RIDs or ORCIDs.
    - requires an email
    - requires a UnifiedOrg to be related to a pub
    :return: Graph as nt file
    """
    q = rq_prefixes + """
    #select ?dais (SAMPLE(?fullName) AS ?name) (group_concat(distinct ?aship ; separator = "|") AS ?contrib)
    select 
        (COUNT(?dais) as ?num)
        ?dais 
        (SAMPLE(?fullName) AS ?name) 
        (SAMPLE(?first) AS ?firstName) 
        (SAMPLE(?last) AS ?lastName) 
        (group_concat(distinct ?email ; separator = "|") AS ?emails) 
        (group_concat(distinct ?aship ; separator = "|") AS ?authorships)
    where {
        ?aship a vivo:Authorship ;
            wos:fullName ?fullName ;
            rdfs:label ?label ;
            wos:daisNg ?dais ;
            wos:firstName ?first ;
            wos:lastName ?last ;
            wos:email ?email ;
            vivo:relates ?addr .
    }
    GROUP BY ?dais
    """
    vstore = backend.get_store()
    with open(ORCID_FILE) as inf:
        d_to_o = json.load(inf)
    with open(RID_FILE) as inf:
        d_to_r = json.load(inf)
    with open(AU_ID_FILE) as inf:
        auid_to_dais = json.load(inf)
    done = []
    g = Graph()
    for person in vstore.query(q):
        orcid = None
        rid = None
        dais = person.dais.toPython()
        if dais in done:
            continue
        name = person.name.toPython()
        orcids = d_to_o.get(dais)
        rids = d_to_r.get(dais)
        if orcids is not None:
            if len(orcids) > 1:
                logger.info("Ignoring {}. Multiple ORCIDs found.".format(dais))
                continue
            orcid = orcids[0]
        elif rids is not None:
            if len(rids) > 1:
                logger.info("Ignoring {}. Multiple RIDs found.".format(dais))
                continue
            rid = rids[0]
        else:
            logger.info("Skipping {} - no RID or ORCID".format(dais))
            continue
        dais_ids = auid_to_dais[orcid or rid]
        logger.info("Building profile for {} with {}.".format(name, orcid or rid))
        done += dais_ids
        vper = person.asdict()
        vper['dais'] = dais_ids
        vper = Researcher(person)
        if orcid is not None:
            g.add((vper.uri, WOS.orcid, Literal(orcid)))
        elif rid is not None:
            g.add((vper.uri, VIVO.researcherId, Literal(rid)))
        g += vper.to_rdf()
        # authorship
        for aship in person.authorships.split("|"):
            g.add((URIRef(aship), VIVO.relates, vper.uri))
    backend.sync_updates("http://localhost/data/people-identifiers", g)


def build_email_profiles():
    """
    Builds profiles for researchers with emails and a minimum number of publications.
    """
    q = rq_prefixes + """
        select 
            (COUNT(?aship) as ?num)
            ?email
            (SAMPLE(?fullName) AS ?name) 
            (SAMPLE(?first) AS ?firstName) 
            (SAMPLE(?last) AS ?lastName) 
            (group_concat(distinct ?daisNg ; separator = "|") AS ?dais) 
            (group_concat(distinct ?aship ; separator = "|") AS ?authorships)
        where {
            ?aship a vivo:Authorship ;
                wos:fullName ?fullName ;
                rdfs:label ?label ;
                wos:daisNg ?daisNg ;
                wos:email ?email ;
                wos:firstName ?first ;
                wos:lastName ?last .
            FILTER NOT EXISTS {
                ?p a foaf:Person ;
                vivo:relatedBy ?aship .
            }
        }
        GROUP BY ?email
        HAVING (?num >= 5)
        ORDER BY DESC(?num)
    """
    logger.info("Email profiles query:\n" + q)
    vstore = backend.get_store()
    g = Graph()
    for person in vstore.query(q):
        name = person.name.toPython()
        email = person.email.toPython()
        logger.info("Building profile for {} with {}.".format(name, email))
        vper = Researcher(person)
        g += vper.to_rdf()
        # authorship
        for aship in person.authorships.toPython().split("|"):
            g.add((URIRef(aship), VIVO.relates, vper.uri))
    backend.sync_updates("http://localhost/data/people-email", g)


def build_unified_affiliation():
    """
    Relate person entities to unified organizations.
    """
    logger.info("Adding unified affiliation.")
    vstore = backend.get_store()
    q = rq_prefixes + """
    CONSTRUCT {
        ?person a wos:ExternalResearcher .
        ?person wos:hasAffiliation ?unifOrg .
    }
    WHERE {
        ?unifOrg a wos:UnifiedOrganization ;
               vivo:relatedBy ?address .
        ?address a wos:Address ;
               vivo:relatedBy ?authorship ;
               vivo:relates ?unifOrg .
        ?authorship a vivo:Authorship ;
                vivo:relates ?person .
        ?person a foaf:Person .
        FILTER (?unifOrg != d:org-technical-university-of-denmark)
    }
    """
    logger.info("Affiliation query:\n" + q)

    g = vstore.query(q).graph
    backend.post_updates(AFFILIATION_NG, g)


def build_dtu_people():
    """
    Relate person entities to unified organizations.
    """
    logger.info("Adding DTUResearcher type.")
    vstore = backend.get_store()
    q = rq_prefixes + """
    CONSTRUCT {
        ?person a wos:DTUResearcher
    }
    WHERE {
        d:org-technical-university-of-denmark a wos:UnifiedOrganization ;
               vivo:relatedBy ?address .
        ?address a wos:Address ;
               vivo:relatedBy ?authorship ;
               vivo:relates ?unifOrg .
        ?authorship a vivo:Authorship ;
                vivo:relates ?person .
        ?person a foaf:Person .
    }
    """
    logger.info("DTU people query:\n" + q)

    g = vstore.query(q).graph
    backend.post_updates(AFFILIATION_NG, g)


def remove_internal_external():
    """
    Remove the wos:ExternalResearcher class from those that
    are also DTU reseaarchers.
    """
    logger.info("Removing external researcher from internal researchers.")
    vstore = backend.get_store()
    q = rq_prefixes + """
    CONSTRUCT {
        ?r a wos:ExternalResearcher .
    }
    WHERE {
        ?r a wos:ExternalResearcher, wos:DTUResearcher .
    }
    """
    try:
        g = vstore.query(q).graph
        vstore.bulk_remove(AFFILIATION_NG, g)
    except ResultException:
        pass


def index():
    data_files = publications.get_data_files()
    dais_orcid = defaultdict(list)
    dais_rid = defaultdict(list)
    au_identifier_to_dais = defaultdict(list)
    for n, pfile in enumerate(data_files):
        with open(pfile) as inf:
            raw = inf.read()
        pub = publications.WosRecord(raw)
        contrib_idx = index_contributors(pub)
        if contrib_idx is None:
            continue
        for au in pub.authors():
            last = au['last']
            cm = contrib_idx.get(last)
            if cm is None:
                continue
            elif len(cm) == 1:
                dais = au['dais_ng']
                if dais is None:
                    continue
                orcid = cm[0]['orcid']
                rid = cm[0]['r_id']
                if (orcid is not None) and (orcid not in dais_orcid[dais]):
                    dais_orcid[dais].append(orcid)
                    au_identifier_to_dais[orcid].append(dais)
                if (rid is not None) and (rid not in dais_rid[dais]):
                    dais_rid[dais].append(rid)
                    au_identifier_to_dais[rid].append(dais)
            elif len(cm) > 0:
                logger.info("Multiple last name match for UT {} and name {}.".format(pub.ut, au['display_name']))
                raise Exception("Multiple matches")

    with open(ORCID_FILE, 'wb') as out_file:
        json.dump(dais_orcid, out_file)

    with open(RID_FILE, 'wb') as out_file:
        json.dump(dais_rid, out_file)

    with open(AU_ID_FILE, 'wb') as out_file:
        json.dump(au_identifier_to_dais, out_file)


if __name__ == "__main__":
    index()
    build_orcid_rid_profiles()
    build_email_profiles()
    build_unified_affiliation()
    build_dtu_people()
    remove_internal_external()