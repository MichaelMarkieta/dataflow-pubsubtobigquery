### Cloud Pub/Sub to BigQuery

##### Deploy App Engine app with cron job that publishes 1 message every minute to Cloud PubSub

Update js/app.yaml before proceeding with project and pub/sub topic name

```bash
js$ gcloud app deploy app.yaml
js$ gcloud app deploy cron.yaml
```

##### Deploy Cloud Dataflow pipeline

```bash
pubsubtobigquery$ exec:java -Dexec.mainClass=pubsubtobigquery.Pipeline -e "-Dexec.args= --project={PROJECT_ID} --topic={TOPIC} --tempLocation={GCS_TEMP_FOLDER} --runner=DataflowRunner --streaming=true --numWorkers=1 --zone=us-central1-f --workerMachineType=n1-standard-1"
```