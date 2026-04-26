# Sports Analytics Platform - Terraform Infrastructure

This Terraform project uses one existing remote backend storage account:

- Resource group: `rg-tf-state`
- Storage account: `tfstateramanuj`
- Container: `tfstate`

The state files are created automatically by Terraform inside the `tfstate` container:

- `1.bootstrap.tfstate`
- `2.sql-database.tfstate`
- `3.function-app.tfstate`
- `4.static-web-app.tfstate`

There is no `0.CreateState` folder. The backend storage is assumed to already exist.

## Before running

Make sure the `tfstate` container exists in the `tfstateramanuj` storage account.

```bash
az storage container create \
  --name tfstate \
  --account-name tfstateramanuj \
  --auth-mode login
```

Update `env/local.config.json`, especially:

- `sql_admin_password`
- resource names if they already exist or need to be globally unique

## Run order

```bash
./tf.sh 1 init
./tf.sh 1 apply

./tf.sh 2 init
./tf.sh 2 apply

./tf.sh 3 init
./tf.sh 3 apply

./tf.sh 4 init
./tf.sh 4 apply
```

You can also run manually:

```bash
cd 1.bootstrap
terraform init
terraform plan
terraform apply
```

No `-var-file` is needed. Each folder reads values from:

```text
env/local.config.json
```

## Folder purpose

| Folder | Purpose |
|---|---|
| `1.bootstrap` | Resource group, ADLS Gen2, function storage, Key Vault, App Insights |
| `2.sql-database` | Azure SQL Server and Basic SQL Database |
| `3.function-app` | Linux Consumption Azure Function App and RBAC |
| `4.static-web-app` | Azure Static Web App Free tier |

## Important notes

- No `versions.tf` is used. Provider and backend config are in each folder's `provider.tf`.
- There are no Terraform variables unless absolutely needed. Shared settings are read from JSON locals.
- The Azure Storage Account names in `env/local.config.json` must be globally unique and can only contain lowercase letters and numbers.
