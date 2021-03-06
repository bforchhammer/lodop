#!/bin/bash
cd "$(dirname "$0")/.."
RESTART="$(dirname "$0")/scripts/cluster-restart-mapred.sh"

SCRIPTS="number_of_triples_by_resource average_number_of_triples_by_resource average_number_of_triples_per_resource_by_class average_number_of_property_values_by_class"
DATASETS="dbpedia-10M freebase-10M species-1M wdc-rdfa-10M"
OUTPUT="aggregations-evaluation"
NAME="aggregations-evaluation-ex4"

# with pig MQ
$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-normal -s $SCRIPTS -o $OUTPUT"

$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-optimized -s $SCRIPTS -o $OUTPUT --optimize-aggregations"

# without pig MQ
$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-normal-nopigmq -s $SCRIPTS -o $OUTPUT --disable-pig-mqo"

$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-optimized-nopigmq -s $SCRIPTS -o $OUTPUT --optimize-aggregations --disable-pig-mqo"
