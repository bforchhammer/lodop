#!/bin/bash
cd "$(dirname "$0")/.."
RESTART="$(dirname "$0")/scripts/cluster-restart-mapred.sh"

# Aggregation example from Masterthon
SCRIPTS="average_number_of_property_values_by_class average_number_of_property_values_by_property average_number_of_property_values_by_class_and_property"
DATASETS="dbpedia-10M freebase-10M wdc-rdfa-10M dbpedia-1M freebase-1M species-1M wdc-rdfa-1M"
OUTPUT="masterthon-example"
NAME="masterthon-example"

# with pig MQ
$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-merged -s $SCRIPTS -o $OUTPUT"

$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-identicals -s $SCRIPTS -o $OUTPUT --optimize-identicals"

$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-aggregations -s $SCRIPTS -o $OUTPUT --optimize-aggregations"

# without pig MQ
$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-merged-nopigmq -s $SCRIPTS -o $OUTPUT --disable-pig-mqo"

$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-identicals-nopigmq -s $SCRIPTS -o $OUTPUT --optimize-identicals --disable-pig-mqo"

$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-aggregations-nopigmq -s $SCRIPTS -o $OUTPUT --optimize-aggregations --disable-pig-mqo"
