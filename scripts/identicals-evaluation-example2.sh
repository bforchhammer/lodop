#!/bin/bash
cd "$(dirname "$0")/.."
RESTART="$(dirname "$0")/scripts/cluster-restart-mapred.sh"

SCRIPTS="*cooc* uri_literal_ratio_by_class -i"
DATASETS="dbpedia-1M freebase-1M species-1M wdc-rdfa-1M"
OUTPUT="identicals-evaluation"
NAME="identicals-evaluation-ex2-non-complex-1M"

# with pig MQ
$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-normal -s $SCRIPTS -o $OUTPUT"

$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-optimized -s $SCRIPTS -o $OUTPUT --optimize-identicals"

# without pig MQ
$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-normal-nopigmq -s $SCRIPTS -o $OUTPUT --disable-pig-mqo"

$RESTART
./gradlew :benchmark:run -PappArgs="-c -m -d $DATASETS -n $NAME-optimized-nopigmq -s $SCRIPTS -o $OUTPUT --optimize-identicals --disable-pig-mqo"
