
import glob
import os

from slugify import slugify


from settings import PUBS_PATH, RDF_PATH, STAGING_PATH, INCITES_PATH
from namespaces import D


def get_env(key):
    try:
        return os.environ[key]
    except KeyError:
        raise Exception("Required environment variable not found: {}.".format(key))


def mk_paths(main, release):
    p = os.path.join(main, "v{}".format(str(release)))
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


def get_incites_base_path(release):
    return mk_paths("incites", release)

def get_incites_output_path(release, ic_type, org_id):
    bp = mk_paths(INCITES_PATH, release)
    p = os.path.join(bp, ic_type)
    if not os.path.exists(p):
        os.mkdir(p)
    output_file = os.path.join(p, "{}.json".format(org_id))
    return output_file


def get_category_uri(name):
    return D['wosc-' + slugify(name)]
