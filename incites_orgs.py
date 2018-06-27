"""
Get organizations from raw RDF.
"""
import argparse
import csv
import sys

from rdflib import Graph

from namespaces import WOS, rq_prefixes
import settings

logger = settings.get_logger()

def main():
    parser = argparse.ArgumentParser(description='Get orgs')
    parser.add_argument("organizations_file")
    parser.add_argument("output_file")
    parser.add_argument("--format", default="nt")
    args = parser.parse_args()
    g = Graph().parse(args.organizations_file, format=args.format)
    rsp = g.query(rq_prefixes + "SELECT ?uri ?name WHERE {?uri a wos:UnifiedOrganization; rdfs:label ?name}")

    with open(args.output_file, "w") as of:
        writer = csv.writer(of)
        writer.writerow(["rap_id", "name", "uri"])

        n = 0
        for row in rsp:
            uri = row.uri.toPython()
            rap_id = uri.split('/')[-1]
            name = row.name.toPython()
            writer.writerow([rap_id, name, uri])
            n += 1

    logger.info("Found {} organizations.".format(n))

if __name__ == "__main__":
    main()
