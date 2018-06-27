#!/bin/bash

set -e

#
# Script to run luigi with central scheduler
# and post data to configured VIVO.
#

# sample size. Set to -1 to process all
sample=-1

export PYTHONWARNINGS="ignore"

python fetch_pubs_xml.py --start $1 --end $2 -q OG="$3" --out data/pubs

pid=data/tmp/luigi.pid

luigid --background --pidfile ${pid} --logdir data/tmp/luigi.log --state-path data/tmp/state.ld
python tasks.py --sample=${sample}

kill `head -n 1 ${pid}`

python incites_orgs.py data/rdf/unified-orgs.nt data/incites_orgs.txt


