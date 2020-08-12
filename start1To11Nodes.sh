#!/usr/bin/env bash

LOG=${1?Error: no log file given}

memory="28G"

for n in {11..1}; do
	for i in {1..3}; do
	  logfile="${resultfolder}/${dataset}-${n}nodes.log"

	  echo ""
	  echo ""
	  echo "Running mapreduceNNDescent with ${n}  nodes"

	  e_cores=20
	  total_e_cores=$(( n * e_cores ))

	  /opt/spark/2.4.4/bin/spark-submit \
	  --master spark://odin01:7077 \
	  --class "com.github.julkw.mapreducenndescent.MapReduceNNDescent" \
	  --driver-memory "${memory}" \
	  --executor-memory "${memory}" \
	  --num-executors "${n}" \
	  --executor-cores "${e_cores}" \
	  --total-executor-cores "${total_e_cores}" \
	  target/scala-2.11/MapReduceNNDescent-assembly-0.1.jar 
	done
done | tee $LOG