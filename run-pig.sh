# gcloud dataproc jobs submit pig --cluster=hadoop-test --file=script.pig
rm -fr target
pig ranker/pig/main.py data/data.txt
