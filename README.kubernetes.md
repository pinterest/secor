
# Secor on kubernetes / GKE

This document contains notes for setting up secor on kuberntes / GKE and describing it's setup.
This is not a magic "spin it up and it works for everyone" tutorial/doc, but act as a starting point.


# Setup

## Create service account for secor

You will need a service account for accessing other APIs in the GCP ecosystem.[1]

```bash
gcloud beta iam service-accounts create secor-service --display-name "Secor service account"
gcloud beta iam service-accounts keys create --iam-account "secor-service@demo.iam.gserviceaccount.com" "secor.key.json"
```

[1] Creating a specific service account for your service from the scratch has the benefits of clearly documenting its requirements and you can easily grant/revoke authorization specific to the service.

# Log bucket

```bash
gsutil mb gs://logs-secor-demo
```

## Grant access

```bash
gcloud projects add-iam-policy-binding demo --member serviceAccount:secor-service@demo.iam.gserviceaccount.com --role roles/storage.objectAdmin
```

```bash
gsutil iam ch serviceAccount:secor-service@demo.iam.gserviceaccount.com:objectViewer gs://logs-secor-demo
gsutil iam ch serviceAccount:secor-service@demo.iam.gserviceaccount.com:objectCreator gs://logs-secor-demo
```

You can view ACL by issuing:

```bash
gsutil iam get gs://logs-secor-demo
```

# Secrets

```bash
kubectl create secret generic secor-service-account --from-file="service-account.json=./secor.key.json" 
```

# Plain kubernetes manifests

## Config maps

```bash
kubectl create configmap secor-config --from-file=src/main/config/ --dry-run -o yaml | kubectl apply -f -

# OR

kubectl create configmap secor-config --from-file=deploys/helm/secor/config/ --dry-run -o yaml | kubectl apply -f -

```

In the end you will have the following config maps in your namespace:


* secor-config
* secorns-envvars-backup-common
* secorns-envvars-backup-log4j
* secorns-envvars-backup-monitor-log4j
* secorns-envvars-partition-common
* secorns-envvars-partition-log4j
* secorns-envvars-partition-monitor-log4j

The `-common` config file specify the required `ZOOKEEPR_QUORUM` and `ZOOKEEPER_PATH` as required by `docker-entrypoint.sh` and selects a `CONFIG_FILE` that should be populated by the `secor-config` which is a ConfigMap volume mount.

The `-partition-*-log4j` config map holds the `LOG4J_CONFIGURATION` environment variable to change which log4j configuration file you want to be used by the secor partition consumer. This should be populated by the `secor-config` which is a ConfigMap volume mount.

The `-backup-*-log4j` config map holds the `LOG4J_CONFIGURATION` environment variable to change which log4j configuration file you want to be used by the secor backup consumer. This should be populated by the `secor-config` which is a ConfigMap volume mount.

`secor-config` is volume mounted under `/opt/secor/config` inside the pod.


# Helm notes


## Config 

Different from plain manifests here is that there is only one config map which is volume mounted in
/opt/secor/config and charts sets environment variables directly via helm
values.

## Deploy

Zedge uses https://github.com/zedge/kubecd to apply cluster state to environments,
this is from `kcd dump <cluster>` which prints helm commands required for applying the cluster state:

```bash
$ pwd => secor.git/deploys/helm .. 

helm repo add stable https://kubernetes-charts.storage.googleapis.com/
helm repo add incubator https://kubernetes-charts-incubator.storage.googleapis.com/
Environment: test
gcloud container clusters get-credentials --project demo --zone us-central1-b test-cluster
kubectl config set-context env:test --cluster gke_demo_us-central1-b_test-cluster --user gke_demo_us-central1-b_test-cluster --namespace default
helm --kube-context env:test upgrade secor secor -i --namespace default --set image.prefix=us.gcr.io/demo/,ingress.domain=demo.example.net --values values-secor.yaml --set nameOverride=secor,image.tag=latest,exporter.image.tag=v0.6.0
```


# Shared notes


## secor.local.path

Currently you should configure `secor.local.path` for `backup group` to be `/mnt/secor_data/message_logs/backup/`
and for `partition group` to be `/mnt/secor_data/message_logs/partition`.

If you want to change this, you probably want to enhance the kubernetes manifests, which you might be interested
in doing anyway, depending on what requirement you have for how large the persistent volume claims should be. 

## Manifests notes

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

##  Metrics

Manually browsing metric endpoint is done by:

kubectl port-forward secor-0 9102
Access on http://localhost:9102/metrics 

Of course in production you should have prometheus to pull metrics automatically.
