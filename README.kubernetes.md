
# Create service account for secor

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


