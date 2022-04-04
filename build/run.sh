spark-submit --deploy-mode cluster \
--master yarn \
--num-executors 3 \
--executor-cores 3 \
--executor-memory 3g \
#-–conf spark.yarn.submit.waitAppCompletion=false \
aws_pyspark_test.py
