#!/bin/bash

set -e

#
# Script to run luigi with central scheduler
# and post data to configured VIVO.
#

export PYTHONWARNINGS="ignore"

# sample size. Set to -1 to process all
sample=200

pid=data/tmp/luigi.pid

luigid --background --pidfile ${pid} --logdir data/tmp/luigi.log --state-path data/tmp/state.ld
python tasks.py --sample=${sample}

kill `head -n 1 ${pid}`

python post_rdf.py --path $1


