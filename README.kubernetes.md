
# Secor on kubernetes / GKE

This document contains notes for setting up secor on kuberntes / GKE and describing it's setup.

# Setup

## Create service account for secor

You will need a service account for accessing other APIs in the GCP ecosystem.[1]

```bash
gcloud beta iam service-accounts create secor-service --display-name "Secor service account"
gcloud beta iam service-accounts keys create --iam-account "secor-service@zedge-dev.iam.gserviceaccount.com" "secor.key.json"
```

[1] Creating a specific service account for your service from the scratch has the benefits of clearly documenting its requirements and you can easily grant/revoke authorization specific to the service.

# Log bucket

```bash
gsutil mb gs://zedge-logs-secor-norangshol
```

## Grant access

```bash
gcloud projects add-iam-policy-binding zedge-dev --member serviceAccount:secor-service@zedge-dev.iam.gserviceaccount.com --role roles/storage.objectAdmin
```

```bash
gsutil iam ch serviceAccount:secor-service@zedge-dev.iam.gserviceaccount.com:objectViewer gs://zedge-logs-secor-norangshol
gsutil iam ch serviceAccount:secor-service@zedge-dev.iam.gserviceaccount.com:objectCreator gs://zedge-logs-secor-norangshol
```

You can view ACL by issuing:

```bash
gsutil iam get gs://zedge-logs-secor-norangshol
```

# Secrets

```bash
kubectl create secret generic secor-service-account --from-file="service-account.json=./secor.key.json" 
```

# Config maps

For zedge this is maintained in https://github.com/zedge/secor-config , but this is as simple as:

```bash
kubectl create configmap config --from-file=src/main/config/ --dry-run -o yaml | kubectl apply -f -
```

# Manifests notes

Currently secor is deployed using a statefulset in kubernetes to gain stable
network identifiers and voulme template claims for storing the local data files
stored under `secor.local.path`.

## Why not Deployment as workload?

We can research later to work with pure kubernetes deployments for easier scaling of secor,
but currently you will get a lot of warnings with "No writer found for path" because it finds
 references to old pods I assume via zookeeper for trying to clean up files locally stored
 under `secor.local.path`.

Current assumptions by norangshol is that you can get rid of this by having stable consumer idenitfier,
and these aren't currently stable when using workload deployment as hostname identifiers contains
 random parts which again is used for keeping track of consumers offsets in zookeeper.


## Containers

Secor is being deployed with 3 containers, one is the main partition consumer which writes logs to GCS,
and 2 other side cars which provides a statsd-prometheus service and a monitor side car running the
 `ProgressMonitorMain` to feed the statsd-prometheus exporter so metrics can be consumed via an prometheus scraper.


