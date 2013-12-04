#!/bin/bash
cd "$(dirname "$0")"

./filters-evaluation.sh
./aggregations-evaluation.sh
