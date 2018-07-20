import os

from settings import PUBS_PATH, RDF_PATH, STAGING_PATH


def _do_paths(main, release):
    p = os.path.join(main, str(release))
    if not os.path.exists(main):
        os.mkdir(main)
    if not os.path.exists(p):
        os.mkdir(p)
    return p


def get_pubs_base_path(release):
    return _do_paths(PUBS_PATH, release)


def get_rdf_path(release):
    return _do_paths(RDF_PATH, release)


def get_staging_path(release):
    return _do_paths(STAGING_PATH, release)