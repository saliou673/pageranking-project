# gcloud dataproc jobs submit pig --cluster=hadoop-test --file=script.pig
rm ranking-output.txt
spark-submit ranker/spark/main.py data/data.txt 10
