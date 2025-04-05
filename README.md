# DE-project-1

Uploading the dataset to Google Cloud Bucket
```bash
gsutil -m cp -r dataset/ gs://olalekan-de2753/dataset
```
code to download the Spark-bigQuery connector jar file

```
mkdir -p ./jars
curl -L -o ./jars/spark-bigquery-with-dependencies_2.12-0.30.0.jar \
https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.30.0/spark-bigquery-with-dependencies_2.12-0.30.0.jar

```

Moving scripts to gcs

```
gsutil -m cp -r spark/ gs://olalekan-de2753/script/
```
