#!/usr/bin/env bash

java -Dspring.hadoop.resourceManagerHost=sandbox-hdp.hortonworks.com \
-Dspring.hadoop.fsUri=hdfs://sandbox-hdp.hortonworks.com \
-Dspring.yarn.client.launchcontext.options="-Dread.path=/user/aliaksei/data/train.csv.gz \
-Dwrite.path=/user/aliaksei/dist \
-Daggregate.parallelism=3 \
-Dspring.hadoop.fsUri=hdfs://sandbox-hdp.hortonworks.com \
-Dspring.yarn.appmaster.launchcontext.options="-Dspring.hadoop.fsUri=hdfs://sandbox-hdp.hortonworks.com"" \
-jar yarn-task-client-1.0.jar
