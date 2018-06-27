"""
Ingest Tasks

Map WOS XML to RDF for RAP.

"""

import argparse
import csv
import os
import sys

import luigi
from rdflib import Graph, Literal, URIRef

from namespaces import D, WOS, RDFS, RDF, SKOS
from settings import logger, RDF_PATH

from lib import backend

from publications import (
    RDFRecord,
    sample_data_files,
    get_data_files,
    add_author_keyword_data_property,
    add_keyword_plus_data_property,
    slug_uri,
    add_grant
)


def get_out_path(name):
    return os.path.join(RDF_PATH, name)


def yield_files(sample):
    if sample == -1:
        file_names = get_data_files()
    else:
        file_names = sample_data_files(sample)
    for fn in file_names:
        with open(fn) as inf:
            raw = inf.read()
            rec = RDFRecord(raw)
            yield rec


class Base(luigi.Task):
    def serialize(self, graph):
        # post - VIVO doesn't handle concurrent writes well
        # named_graph = self.NG_BASE + self.output().path.split("/")[-1].split(".")[0]
        # logger.info("Syncing graph to {}.".format(named_graph))
        # added, removed = backend.sync_updates(named_graph, graph)

        # write to file
        with self.output().open('w') as out_file:
            raw = graph.serialize(format='nt')
            out_file.write(raw)


class DoPubs(Base):
    sample = luigi.IntParameter()

    def run(self):
        g = Graph()
        for rec in yield_files(self.sample):
            logger.debug("Mapping {} to RDF.".format(rec.ut))
            g += rec.to()

        self.serialize(g)

    def output(self):
        path = get_out_path("pubs.nt")
        return luigi.LocalTarget(path)


class DoVenues(Base):
    sample = luigi.IntParameter()

    def run(self):
        g = Graph()
        for rec in yield_files(self.sample):
            logger.info("Mapping {} to RDF.".format(rec.ut))
            g += rec.venue()

        self.serialize(g)

    def output(self):
        path = get_out_path("venues.nt")
        return luigi.LocalTarget(path)


class DoAuthorship(Base):
    sample = luigi.IntParameter()

    def run(self):
        g = Graph()
        for rec in yield_files(self.sample):
            logger.info("Mapping {} to RDF.".format(rec.ut))
            g += rec.authorships()

        self.serialize(g)

    def output(self):
        path = get_out_path("authorship.nt")
        return luigi.LocalTarget(path)


class DoAddress(Base):
    sample = luigi.IntParameter()

    def run(self):
        g = Graph()
        for rec in yield_files(self.sample):
            logger.info("Mapping {} to RDF.".format(rec.ut))
            g += rec.addressships()

        self.serialize(g)

    def output(self):
        path = get_out_path("address.nt")
        return luigi.LocalTarget(path)


class DoSubOrgs(Base):
    sample = luigi.IntParameter()

    def run(self):
        g = Graph()
        for rec in yield_files(self.sample):
            logger.info("Mapping {} to RDF.".format(rec.ut))
            g += rec.sub_orgs()

        self.serialize(g)

    def output(self):
        path = get_out_path("suborgs.nt")
        return luigi.LocalTarget(path)


class DoUnifiedOrgs(Base):
    sample = luigi.IntParameter()

    def run(self):
        g = Graph()
        for rec in yield_files(self.sample):
            logger.info("Mapping {} to RDF.".format(rec.ut))
            g += rec.unified_orgs()

        self.serialize(g)

    def output(self):
        path = get_out_path("unified-orgs.nt")
        return luigi.LocalTarget(path)


class DoCategories(Base):
    sample = luigi.IntParameter()

    def run(self):
        g = Graph()
        for rec in yield_files(self.sample):
            logger.info("Mapping {} to RDF.".format(rec.ut))
            g += rec.categories_g()

        self.serialize(g)

    def output(self):
        path = get_out_path("categories-pubs.nt")
        return luigi.LocalTarget(path)


class KeywordsPlus(Base):

    sample = luigi.IntParameter()

    def run(self):
        kwp_g = Graph()
        logger.info("Indexing publication keywords")
        for rec in yield_files(self.sample):
            for kwp in rec.keywords_plus():
                kwp_g += add_keyword_plus_data_property(kwp, rec.uri)

        self.serialize(kwp_g)

    def output(self):
        path = get_out_path("keywords-plus.nt")
        return luigi.LocalTarget(path)


class AuthorKeywords(Base):

    sample = luigi.IntParameter()

    def run(self):
        outg = Graph()
        logger.info("Indexing publication keywords")
        for rec in yield_files(self.sample):
            for kw in rec.author_keywords():
                outg += add_author_keyword_data_property(kw, rec.uri)

        self.serialize(outg)

    def output(self):
        path = get_out_path("author-keywords.nt")
        return luigi.LocalTarget(path)


class Grants(Base):

    sample = luigi.IntParameter()

    def run(self):
        g = Graph()
        logger.info("Indexing grants")
        for rec in yield_files(self.sample):
            for grant in rec.grants():
                g += add_grant(grant, rec.uri)

        self.serialize(g)

    def output(self):
        path = get_out_path("grants.nt")
        return luigi.LocalTarget(path)


class DoPubProcess(luigi.Task):
    sample = luigi.IntParameter()

    def requires(self):
        yield DoPubs(sample=self.sample)
        yield DoVenues(sample=self.sample)
        yield DoAuthorship(sample=self.sample)
        yield DoAddress(sample=self.sample)
        yield DoSubOrgs(sample=self.sample)
        yield Grants(sample=self.sample)
        yield DoUnifiedOrgs(sample=self.sample)
        yield DoCategories(sample=self.sample)
        yield KeywordsPlus(sample=self.sample)
        yield AuthorKeywords(sample=self.sample)

if __name__ == '__main__':
    #"--local-scheduler",
    parser = argparse.ArgumentParser(description='Map WOS documents to RDF')
    parser.add_argument('--sample', '-s', default=500, type=int, help="Sample size")
    parser.add_argument('--local', '-l', default=False, action="store_true", help="Use local scheduler")
    parser.add_argument('--workers', '-w', default=3, help="luigi workers")
    args = parser.parse_args(sys.argv[1:])

    params = ["--sample={}".format(args.sample), "--workers={}".format(args.workers)]
    if args.local is True:
        params.append("--local-scheduler")
    luigi.run(params, main_task_cls=DoPubProcess)
