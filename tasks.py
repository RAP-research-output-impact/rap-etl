"""
Ingest Tasks

Map WOS XML to RDF for RAP.

"""

import argparse
import glob
import os
import sys

import luigi
from rdflib import Graph

from lib import utils
from settings import logger, DATA_RELEASE

from publications import (
    RDFRecord,
    add_author_keyword_data_property,
    add_keyword_plus_data_property,
    add_grant
)


def yield_files(release):
    pubs_dir = utils.get_pubs_base_path(release)
    p = os.path.join(pubs_dir, '*', '*.xml')
    file_names = [f for f in glob.glob(p)]
    for fn in file_names:
        with open(fn) as inf:
            raw = inf.read()
            rec = RDFRecord(raw)
            yield rec


class Base(luigi.Task):
    release = luigi.IntParameter()

    def get_out_path(self, name):
        rdf_dir = utils.get_rdf_path(self.release)
        p = os.path.join(rdf_dir, name)
        return p

    def serialize(self, graph):
        # write to file
        with self.output().open('w') as out_file:
            raw = graph.serialize(format='nt')
            out_file.write(raw)


class DoPubs(Base):

    def run(self):
        logger.info("Indexing publications")
        g = Graph()
        for rec in yield_files(self.release):
            logger.debug("Mapping base publication {} to RDF.".format(rec.ut))
            g += rec.to()

        self.serialize(g)

    def output(self):
        path = self.get_out_path("pubs.nt")
        return luigi.LocalTarget(path)


class DoVenues(Base):

    def run(self):
        logger.info("Indexing venues")
        g = Graph()
        for rec in yield_files(self.release):
            logger.debug("Mapping venues {} to RDF.".format(rec.ut))
            g += rec.venue()

        self.serialize(g)

    def output(self):
        path = self.get_out_path("venues.nt")
        return luigi.LocalTarget(path)


class DoAuthorship(Base):

    def run(self):
        logger.info("Indexing authorship")
        g = Graph()
        for rec in yield_files(self.release):
            logger.debug("Mapping authorship {} to RDF.".format(rec.ut))
            g += rec.authorships()

        self.serialize(g)

    def output(self):
        path = self.get_out_path("authorship.nt")
        return luigi.LocalTarget(path)


class DoAddress(Base):

    def run(self):
        logger.info("Indexing addresses")
        g = Graph()
        for rec in yield_files(self.release):
            logger.debug("Mapping addresses {} to RDF.".format(rec.ut))
            g += rec.addressships()

        self.serialize(g)

    def output(self):
        path = self.get_out_path("address.nt")
        return luigi.LocalTarget(path)


class DoSubOrgs(Base):

    def run(self):
        logger.info("Indexing sub orgs")
        g = Graph()
        for rec in yield_files(self.release):
            logger.debug("Mapping sub orgs {} to RDF.".format(rec.ut))
            g += rec.sub_orgs()

        self.serialize(g)

    def output(self):
        path = self.get_out_path("suborgs.nt")
        return luigi.LocalTarget(path)


class DoUnifiedOrgs(Base):

    def run(self):
        logger.info("Indexing unified orgs")
        g = Graph()
        for rec in yield_files(self.release):
            logger.debug("Mapping unified orgs {} to RDF.".format(rec.ut))
            g += rec.unified_orgs()

        self.serialize(g)

    def output(self):
        path = self.get_out_path("unified-orgs.nt")
        return luigi.LocalTarget(path)


class DoCategories(Base):

    def run(self):
        logger.info("Indexing categories")
        g = Graph()
        for rec in yield_files(self.release):
            logger.debug("Mapping categories {} to RDF.".format(rec.ut))
            g += rec.categories_g()

        self.serialize(g)

    def output(self):
        path = self.get_out_path("categories-pubs.nt")
        return luigi.LocalTarget(path)


class KeywordsPlus(Base):

    def run(self):
        kwp_g = Graph()
        logger.info("Indexing keywords plus")
        for rec in yield_files(self.release):
            logger.debug("Mapping keywords plus {} to RDF.".format(rec.ut))
            for kwp in rec.keywords_plus():
                kwp_g += add_keyword_plus_data_property(kwp, rec.uri)

        self.serialize(kwp_g)

    def output(self):
        path = self.get_out_path("keywords-plus.nt")
        return luigi.LocalTarget(path)


class AuthorKeywords(Base):

    def run(self):
        outg = Graph()
        logger.info("Indexing author keywords")
        for rec in yield_files(self.release):
            logger.debug("Mapping author keywords {} to RDF.".format(rec.ut))
            for kw in rec.author_keywords():
                outg += add_author_keyword_data_property(kw, rec.uri)

        self.serialize(outg)

    def output(self):
        path = self.get_out_path("author-keywords.nt")
        return luigi.LocalTarget(path)


class Grants(Base):

    def run(self):
        g = Graph()
        logger.info("Indexing grants")
        for rec in yield_files(self.release):
            for grant in rec.grants():
                logger.debug("Mapping grants and funders {} to RDF.".format(rec.ut))
                g += add_grant(grant, rec.uri)

        self.serialize(g)

    def output(self):
        path = self.get_out_path("grants.nt")
        return luigi.LocalTarget(path)


class DoPubProcess(luigi.Task):
    release = luigi.IntParameter()

    def requires(self):
        yield DoPubs(release=self.release)
        yield DoVenues(release=self.release)
        yield DoAuthorship(release=self.release)
        yield DoAddress(release=self.release)
        yield DoSubOrgs(release=self.release)
        yield Grants(release=self.release)
        yield DoUnifiedOrgs(release=self.release)
        yield DoCategories(release=self.release)
        yield KeywordsPlus(release=self.release)
        yield AuthorKeywords(release=self.release)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Map WOS documents to RDF')
    parser.add_argument('--release', '-r', type=int, help="Release number")
    parser.add_argument('--local', '-l', default=False, action="store_true", help="Use local scheduler")
    parser.add_argument('--workers', '-w', default=3, help="luigi workers")
    args = parser.parse_args(sys.argv[1:])

    try:
        rel_info = DATA_RELEASE[args.release]
    except KeyError:
        raise Exception("Release {} not found. Make sure release is specified in settings.DATA_RELEASE".format(args.release))

    params = ["--release={}".format(args.release), "--workers={}".format(args.workers)]
    if args.local is True:
        params.append("--local-scheduler")
    luigi.run(params, main_task_cls=DoPubProcess)
