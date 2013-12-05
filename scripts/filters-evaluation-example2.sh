#!/bin/bash
cd "$(dirname "$0")/.."
RESTART="$(dirname "$0")/scripts/cluster-restart-mapred.sh"

SCRIPTS="number_of_inlinks inverse_properties property_cooc_by_object_uri"
#DATASETS="dbpedia-10M freebase-10M species-1M wdc-rdfa-10M"
DATASETS="dbpedia-10M species-1M wdc-rdfa-10M"
OUTPUT="filters-evaluation"
NAME="filters-evaluation-ex2"

# with pig MQ
$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-normal -s $SCRIPTS -o $OUTPUT"

$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-optimized -s $SCRIPTS -o $OUTPUT --optimize-filters"

# without pig MQ
$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-normal-nopigmq -s $SCRIPTS -o $OUTPUT --disable-pig-mqo"

$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-optimized-nopigmq -s $SCRIPTS -o $OUTPUT --optimize-filters --disable-pig-mqo"
