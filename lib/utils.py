
import glob
import os

from settings import PUBS_PATH, RDF_PATH, STAGING_PATH


def get_env(key):
    try:
        return os.environ[key]
    except KeyError:
        raise Exception("Required environment variable not found: {}.".format(key))


def mk_paths(main, release):
    p = os.path.join(main, str(release))
    if not os.path.exists(main):
        os.mkdir(main)
    if not os.path.exists(p):
        os.mkdir(p)
    return p


def get_pubs_base_path(release):
    return mk_paths(PUBS_PATH, release)


def get_rdf_path(release):
    return mk_paths(RDF_PATH, release)


def get_staging_path(release):
    return mk_paths(STAGING_PATH, release)


def get_release_xml_files(release):
    pp = get_pubs_base_path(release)
    return [f for f in glob.glob(os.path.join(pp, '*/*.xml'))]
