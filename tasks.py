"""
Ingest Tasks
"""
import csv
import os

import luigi
from rdflib import Graph, Literal, URIRef

from namespaces import D, WOS, RDFS, RDF, SKOS
from settings import logger, CACHE_PATH

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


class MapCategoryTree(Base):
    input_file = 'data/wos-categories-ras.csv'

    @staticmethod
    def do_term(term, broader=None, clz=SKOS.Concept, uri_prefix="wosc"):
        clean_term = term.strip("\"")
        g = Graph()
        uri = slug_uri(clean_term, prefix=uri_prefix)
        g.add((uri, RDF.type, clz))
        g.add((uri, RDFS.label, Literal(clean_term)))
        if broader is not None:
            if broader != uri:
                g.add((uri, SKOS.broader, broader))
        return uri, g

    @staticmethod
    def chunk_ras(value):
        grps = value.split('|')
        size = len(grps)
        if size == 2:
            return grps[0], grps[1], None
        elif size == 3:
            return grps[0], grps[1], grps[2]
        else:
            raise Exception("small row")

    def run(self):
        g = Graph()
        wos_top = D['wos-topics']
        g.add((wos_top, RDF.type, WOS.TopTopic))
        g.add((wos_top, RDFS.label, Literal("Web of Science Subject Schemas")))
        with open(self.input_file) as inf:
            for row in csv.DictReader(inf):
                ra = row['Research Area (eASCA)']
                category = row['WoS Category (tASCA)']
                broad, ra1, ra2 = self.chunk_ras(ra)
                broad_uri, cg = self.do_term(broad, clz=WOS.BroadDiscipline)
                g.add((broad_uri, SKOS.broader, wos_top))
                g += cg
                ra1_uri, cg = self.do_term(ra1, broader=broad_uri, clz=WOS.ResearchArea, uri_prefix="wosra")
                g += cg
                ra2_uri = None
                if ra2 is not None:
                    ra2_uri, cg = self.do_term(ra2, broader=ra1_uri, clz=WOS.ResearchArea, uri_prefix="wosra")
                    g += cg
                cat_uri, cg = self.do_term(category, broader=ra2_uri or ra1_uri, clz=WOS.Category)
                g += cg

        self.serialize(g)

    def output(self):
        path = get_out_path("categories-ras.nt")
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
        yield MapCategoryTree()


if __name__ == '__main__':
    #"--local-scheduler",
    luigi.run(["--sample=-1", "--workers=3"], main_task_cls=DoPubProcess)
