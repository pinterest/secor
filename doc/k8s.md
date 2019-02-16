# Secor on kubernetes/GKE

This document contains notes for setting up secor on kubernetes/GKE and
describing it's setup. This is not a magic "spin it up and it works for
everyone" tutorial/doc, but act as a starting point.

# Setup

## Create service account for secor

You will need a service account for accessing other APIs in the GCP ecosystem:

```
$ gcloud beta iam service-accounts create secor-service \
    --display-name "Secor service account"
$ gcloud beta iam service-accounts keys create --iam-account \
    "secor-service@demo.iam.gserviceaccount.com" "secor.key.json"
```

Creating a specific service account for your service from the scratch has the
benefits of clearly documenting its requirements and you can easily grant/revoke
authorization specific to the service.

# Log bucket

    $ gsutil mb gs://logs-secor-demo

## Grant access

```
$ gcloud projects add-iam-policy-binding demo \
    --member serviceAccount:secor-service@demo.iam.gserviceaccount.com \
    --role roles/storage.objectAdmin
$ gsutil iam ch \
    serviceAccount:secor-service@demo.iam.gserviceaccount.com:objectViewer \
    gs://logs-secor-demo
$ gsutil iam ch \
    serviceAccount:secor-service@demo.iam.gserviceaccount.com:objectCreator \
    gs://logs-secor-demo
```

You can view ACL by issuing:

    $ gsutil iam get gs://logs-secor-demo

# Secrets

    $ kubectl create secret generic secor-service-account \
        --from-file="service-account.json=./secor.key.json"

## Config 

It uses one config map which is volume mounted in `/opt/secor/config` and charts
sets environment variables directly via helm values.

## Deploy

Zedge uses https://github.com/zedge/kubecd to apply cluster state to
environments, this is from `kcd dump <cluster>` which prints helm commands
required for applying the cluster state:

```
$ pwd
secor.git/deploys/helm
$ helm repo add stable https://kubernetes-charts.storage.googleapis.com
$ helm repo add incubator \
    https://kubernetes-charts-incubator.storage.googleapis.com
Environment: test
$ gcloud container clusters get-credentials \
    --project demo \
    --zone us-central1-b test-cluster
$ kubectl config set-context env:test \
    --namespace default \
    --cluster gke_demo_us-central1-b_test-cluster \
    --user gke_demo_us-central1-b_test-cluster
$ helm --kube-context env:test upgrade secor secor -i \
    --namespace default \
    --values values-secor.yaml \
    --set image.prefix=us.gcr.io/demo/,ingress.domain=demo.example.net \
    --set nameOverride=secor,image.tag=latest,exporter.image.tag=v0.6.0
```

## secor.local.path

Currently you should configure `secor.local.path` for `backup group` to be
`/mnt/secor_data/message_logs/backup/` and for `partition group` to be
`/mnt/secor_data/message_logs/partition`. If you want to change this, you
probably want to enhance the kubernetes manifests, which you might be interested
in doing anyway, depending on what requirement you have for how large the
persistent volume claims should be.

## Manifests notes

Currently secor is deployed using a statefulset in kubernetes to gain stable
network identifiers and volume template claims for storing the local data files
stored under `secor.local.path`.

## Why not Deployment as workload?

We can research later to work with pure kubernetes deployments for easier
scaling of secor, but currently you will get a lot of warnings with "No writer
found for path" because it finds references to old pods I assume via zookeeper
for trying to clean up files locally stored under `secor.local.path`.

Current assumptions by norangshol is that you can get rid of this by having
stable consumer identifier, and these aren't currently stable when using
workload deployment as hostname identifiers contains random parts which again
is used for keeping track of consumers offsets in zookeeper.

## Containers

Secor is being deployed with 3 containers, one is the main partition consumer
which writes logs to GCS, and 2 other side cars which provides a
statsd-prometheus service and a monitor side car running the
[com.pinterest.secor.main.ProgressMonitorMain] to feed the statsd-prometheus
exporter so metrics can be consumed via an prometheus scraper.

##  Metrics

Manually browsing metric endpoint is done by:

    $ kubectl port-forward secor-0 9102

Access on http://localhost:9102/metrics.

Of course in production you should have prometheus to pull metrics
automatically.

[com.pinterest.secor.main.ProgressMonitorMain]:
src/main/java/com/pinterest/secor/main/ProgressMonitorMain.java
