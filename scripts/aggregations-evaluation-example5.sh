#!/bin/bash
cd "$(dirname "$0")/.."
RESTART="$(dirname "$0")/scripts/cluster-restart-mapred.sh"

SCRIPTS="average_number_of_property_values_by_class average_number_of_property_values_by_property average_number_of_property_values_by_class_and_property"
DATASETS="dbpedia-10M freebase-10M species-1M wdc-rdfa-10M"
OUTPUT="aggregations-evaluation"
NAME="aggregations-evaluation-ex5"

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

