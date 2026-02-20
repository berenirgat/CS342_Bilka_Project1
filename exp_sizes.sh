#!/usr/bin/env bash
set -e

N=3
K=6
DATALEN=1000
REPEAT=5
PREFIX="infile"

SIZES=(10000 100000 1000000 5000000 10000000)

echo "program,size_bytes,run,elapsed_sec" > results_sizes.csv

make_inputs () {
  local size=$1
  for i in 1 2 3; do
    yes "hello world this is a test line another line with hellooooo veryverylongword appears another another" \
    | head -c $size > ${PREFIX}${i}
  done
}

for size in "${SIZES[@]}"; do
  make_inputs $size

  for r in 1 2 3 4 5; do
    
    # Process version
    tp=$(/usr/bin/time -f "%e" ./findlwp $PREFIX $N $K $DATALEN out_proc.txt 2>&1 >/dev/null)
    echo "proc,$size,$r,$tp" >> results_sizes.csv

    # Thread version
    tt=$(/usr/bin/time -f "%e" ./findlwt $PREFIX $N $K out_thread.txt 2>&1 >/dev/null)
    echo "thread,$size,$r,$tt" >> results_sizes.csv

  done
done

echo "DONE"

