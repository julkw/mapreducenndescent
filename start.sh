#!/usr/bin/env bash

memory="28G"

/opt/spark/2.4.4/bin/spark-submit \
  --master spark://odin01:7077 \
  --class "com.github.julkw.mapreducenndescent.MapReduceNNDescent" \
  --driver-memory "${memory}" \
  --executor-memory "${memory}" \
  --num-executors 11 \
  --executor-cores 20 \
  --total-executor-cores 220 \
  target/scala-2.11/MapReduceNNDescent-assembly-0.1.jar 
