"""
Build person profiles using ORCID, ResearcherID, email data from WOS records.

"""
import argparse
from collections import defaultdict
import json
import os

from rdflib import Graph, URIRef, Literal
from rdflib.query import ResultException
from rdflib.resource import Resource

from lib import backend, utils

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
    PEOPLE_IDENTIFIERS_GRAPH,
    PEOPLE_EMAIL_GRAPH,
    PEOPLE_DTU_DAIS_GRAPH,
    PEOPLE_GRAPH,
    ORCID_FILE,
    RID_FILE,
    AU_ID_FILE,
    PEOPLE_AUTHORSHIP,
    RDF_PATH
)

def profile_dir(release):
    profiles = 'data/profiles'
    if not os.path.exists(profiles):
        os.mkdir(profiles)
    profiles = 'data/profiles/{:03d}'.format(release)
    if not os.path.exists(profiles):
        os.mkdir(profiles)
    return profiles

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

class Researcher(object):

    def __init__(self, profile, dais_ids):
        self.profile = profile
        self.dais_ids = dais_ids
        # Use dais ids as URI key. If multiple
        # exist, sort and take the first one.
        dais_ids.sort()
        self.vid = dais_ids[0]
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
        names = data['full_names'].split("|")
        full_name = max(names, key=len)
        person.set(RDFS.label, Literal(full_name.strip()))
        # For sorting
        #person.set(FOAF.familyName, Literal(data['lastName']))
        #person.set(WOS.alphaBrowse, Literal(data['lastName'][0].lower()))

        # dais
        for did in self.dais_ids:
            person.set(WOS.daisNg, Literal(did))

        # Vcard individual
        vci_uri =self.vcard_uri
        person.set(OBO['ARG_2000028'], vci_uri)
        g.add((vci_uri, RDF.type, VCARD.Individual))

        # Vcard Name
        #g += self._vcard_name()
        #g.add((vci_uri, VCARD.hasName, URIRef(self.vcard_name_uri)))

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
            try:
                emails = [self.profile['email']]
            except KeyError:
                emails = []
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


def get_existing_people():
    logger.info("Getting existing profiles.")
    q = rq_prefixes + """
    select ?p
    where
    {
        ?p a foaf:Person
    }
    """
    vstore = backend.get_store()
    out = []
    for row in vstore.query(q):
        out.append(row.p)
    return out


def build_orcid_rid_profiles(release):
    """
    Builds profiles for researchers with RIDs or ORCIDs.
    """
    q = rq_prefixes + """
    select
        (COUNT(?aship) as ?num)
        ?dais
        (group_concat(distinct ?fullName ; separator = "|") AS ?full_names)
        (SAMPLE(?fullName) AS ?name)
        (SAMPLE(?first) AS ?firstName)
        (SAMPLE(?last) AS ?lastName)
    where {
        ?aship a vivo:Authorship ;
            wos:fullName ?fullName ;
            rdfs:label ?label ;
            wos:daisNg ?dais ;
            wos:firstName ?first ;
            wos:lastName ?last ;
            vivo:relates ?addr .
    }
    GROUP BY ?dais
    #HAVING (?num >= 3)
    """
    logger.info("Author ID profiles query:\n" + q)
    vstore = backend.get_store()
    profiles = profile_dir(release)
    with open(os.path.join(profiles, ORCID_FILE)) as inf:
        d_to_o = json.load(inf)
    with open(os.path.join(profiles, RID_FILE)) as inf:
        d_to_r = json.load(inf)
    with open(os.path.join(profiles, AU_ID_FILE)) as inf:
        auid_to_dais = json.load(inf)
    existing = get_existing_people()
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
        for did in dais_ids:
            if did in done:
                continue
        logger.info("Building profile for {} with {}.".format(name, orcid or rid))
        done += dais_ids
        vper = Researcher(person, dais_ids)
        if vper.uri in existing:
            logger.info("Profile exists with URI {}.".format(vper.uri))
            continue
        if orcid is not None:
            g.add((vper.uri, WOS.orcid, Literal(orcid)))
        elif rid is not None:
            g.add((vper.uri, VIVO.researcherId, Literal(rid)))
        g += vper.to_rdf()
    save_rdf(release, g, PEOPLE_IDENTIFIERS_GRAPH)


def build_email_profiles(release):
    """
    Builds profiles for researchers with emails and a minimum number of publications.
    """
    q = rq_prefixes + """
        select
            (COUNT(?aship) as ?num)
            ?email
            (SAMPLE(?fullName) AS ?name)
            (group_concat(distinct ?fullName ; separator = "|") AS ?full_names)
            (SAMPLE(?first) AS ?firstName)
            (SAMPLE(?last) AS ?lastName)
            (group_concat(distinct ?daisNg ; separator = "|") AS ?dais)
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
                    wos:daisNg ?daisNg .
            }
        }
        GROUP BY ?email
        HAVING (?num >= 3)
        ORDER BY DESC(?num)
    """
    logger.info("Email profiles query:\n" + q)
    vstore = backend.get_store()
    existing = get_existing_people()
    g = Graph()
    for person in vstore.query(q):
        name = person.name.toPython()
        email = person.email.toPython()
        dais_ids = [d for d in person.dais.toPython().split("|")]
        logger.info("Building profile for {} with {}.".format(name, email))
        vper = Researcher(person, dais_ids)
        if vper.uri in existing:
            logger.info("Profile exists for {}.".format(vper.uri))
            continue
        g += vper.to_rdf()
#   vstore.bulk_add(PEOPLE_EMAIL_GRAPH, g)
    save_rdf(release, g, PEOPLE_EMAIL_GRAPH)


def build_dais_profiles(release):
    """
    Builds profiles for researchers by DAIS and a minimum number of publications.
    """
    profiles = profile_dir(release)
    with open(os.path.join(profiles, ORCID_FILE)) as inf:
        d_to_o = json.load(inf)
    with open(os.path.join(profiles, RID_FILE)) as inf:
        d_to_r = json.load(inf)
    with open(os.path.join(profiles, AU_ID_FILE)) as inf:
        auid_to_dais = json.load(inf)
    q = rq_prefixes + """
        select
            (COUNT(?aship) as ?num)
            ?dais
            (group_concat(distinct ?fullName ; separator = "|") AS ?full_names)
            (SAMPLE(?fullName) AS ?name)
            (SAMPLE(?first) AS ?firstName)
            (SAMPLE(?last) AS ?lastName)
        where {
            ?aship a vivo:Authorship ;
                vivo:relates ?addr ;
                wos:fullName ?fullName ;
                rdfs:label ?label ;
                wos:daisNg ?dais ;
                wos:firstName ?first ;
                wos:lastName ?last .
        }
        GROUP BY ?dais
        HAVING (?num >= 20)
        ORDER BY DESC(?num)
    """
    q = rq_prefixes + """
        select
            (COUNT(?aship) as ?num)
            ?dais
            (group_concat(distinct ?fullName ; separator = "|") AS ?full_names)
            (SAMPLE(?fullName) AS ?name)
            (SAMPLE(?first) AS ?firstName)
            (SAMPLE(?last) AS ?lastName)
        where {
            ?aship a vivo:Authorship ;
                vivo:relates ?addr ;
                wos:fullName ?fullName ;
                rdfs:label ?label ;
                wos:daisNg ?dais ;
                wos:firstName ?first ;
                wos:lastName ?last .
        }
        GROUP BY ?dais
        ORDER BY DESC(?num)
    """
    logger.info("DAIS profiles query:\n" + q)
    vstore = backend.get_store()
    g = Graph()
    for person in vstore.query(q):
        name = person.name.toPython()
        dais = person.dais.toPython()
        # Add RID, ORCID and additional DAIS if possible.
        orcids = d_to_o.get(dais, [None])
        rids = d_to_r.get(dais, [None])
        if len(orcids) > 1:
            orcid = None
        else:
            orcid = orcids[0]
        if len(rids) > 1:
            rid = None
        else:
            rid = rids[0]
        person_uri = D['person-' + dais]
        dais_ids = [dais]
        if orcid is not None:
            dais_ids += auid_to_dais.get(orcid, [])
        elif rid is not None:
            dais_ids += auid_to_dais.get(rid, [])
        dais_ids = [dais]
        logger.info("Building profile for {} with {}.".format(name, dais))
        vper = Researcher(person, dais_ids)
        g += vper.to_rdf()
        if orcid is not None:
            g.add((vper.uri, WOS.orcid, Literal(orcid)))
        if rid is not None:
            g.add((vper.uri, VIVO.researcherId, Literal(rid)))
    save_rdf(release, g, PEOPLE_GRAPH)


def build_unified_affiliation(release):
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
        FILTER (?unifOrg !=  d:org-technical-university-of-denmark)
        FILTER NOT EXISTS { ?person a wos:ExternalResearcher }
    }
    """
    logger.info("Affiliation query:\n" + q)
    g = vstore.query(q).graph
    save_rdf(release, g, AFFILIATION_NG)


def build_dtu_people(release):
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
               vivo:relates d:org-technical-university-of-denmark .
        ?authorship a vivo:Authorship ;
                vivo:relates ?person .
        ?person a foaf:Person .
    }
    """
    logger.info("DTU people query:\n" + q)

    g = vstore.query(q).graph
    save_rdf(release, g, AFFILIATION_NG)

def remove_internal_external():
    """
    Remove the wos:ExternalResearcher class from those that
    are also DTU researchers.
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


def add_authorship_links(release):
    """
    Create people to authorship links.
    """
    logger.info("Adding additional authorships.")
    vstore = backend.get_store()
    logger.info("Fetching people to authorship batch.")
    q = rq_prefixes + """
        CONSTRUCT {
            ?aship vivo:relates ?p .
        }
        WHERE {
            ?p a foaf:Person ;
                wos:daisNg ?dais .
            ?aship a vivo:Authorship ;
                wos:daisNg ?dais .
            FILTER NOT EXISTS { ?aship vivo:relates ?p }
        }
    """
    logger.info("Authorship query:\n" + q)
    g = vstore.query(q).graph
    save_rdf(release, g, PEOPLE_AUTHORSHIP)

def index(release):
    data_files = utils.get_release_xml_files(release)
    if len(data_files) == 0:
        raise Exception("No XML files found.")
    logger.info("Processing {} publication files.".format(len(data_files)))

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
            dais = au['dais_ng']
            if dais is None:
                continue
            last = au['last']
            contributor_matches = contrib_idx.get(last)
            if contributor_matches is None:
                continue
            elif len(contributor_matches) == 1:
                # Full name match on WOS full name and contributor full name.
                au_name_key = au["full_name"]
                cm_name_key = "{}, {}".format(contributor_matches[0]['last'].encode('utf-8'), contributor_matches[0]['first'].encode('utf-8'))
                orcid = contributor_matches[0]['orcid']
                rid = contributor_matches[0]['r_id']
                if au_name_key.lower() != cm_name_key.lower():
                    logger.debug("{} - Name keys don't match - {} {}".format(orcid or rid, au_name_key, cm_name_key))
                    continue
                if (orcid != "") and (orcid is not None) and (orcid not in dais_orcid[dais]):
                    dais_orcid[dais].append(orcid)
                    au_identifier_to_dais[orcid].append(dais)
                elif (rid != "") and (rid is not None) and (rid not in dais_rid[dais]):
                    dais_rid[dais].append(rid)
                    au_identifier_to_dais[rid].append(dais)
                else:
                    continue
            elif len(contributor_matches) > 1:
                logger.info("Multiple last name match for UT {} and name {}.".format(pub.ut, au['display_name']))
                raise Exception("Multiple matches")
            else:
                raise Exception("Unexpected contributor match count")

    profiles = profile_dir(release)
    with open(os.path.join(profiles, ORCID_FILE), 'wb') as out_file:
        json.dump(dais_orcid, out_file)

    with open(os.path.join(profiles, RID_FILE), 'wb') as out_file:
        json.dump(dais_rid, out_file)

    with open(os.path.join(profiles, AU_ID_FILE), 'wb') as out_file:
        json.dump(au_identifier_to_dais, out_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create researcher profiles.')
    parser.add_argument('--phase', '-p', required=True, type=str, help="Valid phase are: people, id, email, authorship, affiliation, cleanup")
    parser.add_argument('--release', '-r', type=int, help="Release number")
    args = parser.parse_args()

    utils.release(args.release)
    if utils.RELEASE == 0:
        raise Exception("fatal: release not found: {}".format(args.release))
    profiles = profile_dir(utils.RELEASE)
    if not os.path.exists(os.path.join(profiles, ORCID_FILE)):
        index(utils.RELEASE)

    if args.phase == 'people':
        build_dais_profiles(utils.RELEASE)
    elif args.phase == 'id':
        build_orcid_rid_profiles(utils.RELEASE)
    elif args.phase == 'email':
        build_email_profiles(utils.RELEASE)
    elif args.phase == 'authorship':
        add_authorship_links(utils.RELEASE)
    elif args.phase == 'affiliation':
        build_unified_affiliation(utils.RELEASE)
        build_dtu_people(utils.RELEASE)
    elif args.phase == 'cleanup':
        remove_internal_external()
    else:       
        logger.info('unknown phase: {}'.format(args.phase))

