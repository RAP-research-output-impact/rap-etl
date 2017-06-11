"""
Ingest Tasks
"""
import os

import luigi
from rdflib import Graph

from settings import logger, CACHE_PATH

from publications import (
    RDFRecord,
    sample_data_files,
    get_data_files,
    add_author_keyword,
    add_keyword_plus,
)


def get_out_path(name):
    return os.path.join(CACHE_PATH, name)


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
        with self.output().open('w') as out_file:
            raw = graph.serialize(format='nt')
            out_file.write(raw)


class DoPubs(Base):
    sample = luigi.IntParameter()

    def run(self):
        g = Graph()
        for rec in yield_files(self.sample):
            logger.info("Mapping {} to RDF.".format(rec.ut))
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
        path = get_out_path("categories.nt")
        return luigi.LocalTarget(path)


class KeywordsPlus(Base):

    sample = luigi.IntParameter()

    def run(self):
        kwp_g = Graph()
        logger.info("Indexing publication keywords")
        for rec in yield_files(self.sample):
            for kwp in rec.keywords_plus():
                kuri, kg = add_keyword_plus(kwp, rec.uri)
                kwp_g += kg

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
                kuri, kg = add_author_keyword(kw, rec.uri)
                outg += kg

        self.serialize(outg)

    def output(self):
        path = get_out_path("author-keywords.nt")
        return luigi.LocalTarget(path)


class DoPubProcess(luigi.Task):
    sample = luigi.IntParameter()

    def requires(self):
        yield DoPubs(sample=self.sample)
        yield DoVenues(sample=self.sample)
        yield DoAuthorship(sample=self.sample)
        yield DoAddress(sample=self.sample)
        yield DoSubOrgs(sample=self.sample)
        yield DoUnifiedOrgs(sample=self.sample)
        yield DoCategories(sample=self.sample)
        yield KeywordsPlus(sample=self.sample)
        yield AuthorKeywords(sample=self.sample)


if __name__ == '__main__':
    #"--local-scheduler",
    luigi.run(["--sample=500", "--workers=3"], main_task_cls=DoPubProcess)
