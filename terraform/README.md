# Terraform setup in docker
Due to terraform not supporting reading from env file, we are injecting env file's variable through Docker

## Usage 
docker compose run cli [global options] \<subcommand\> [args]

example: \
"terraform apply" is same as "docker compose run cli apply"

## Set up
To set up your cloud infrastructure for the first time, make sure all values in .env file are filled out and copied in this folder. \
Then cd into terraform folder and input "docker compose run cli apply"

It is recommended to do \
"docker compose run cli destroy --target google_dataproc_cluster.spark_cluster" \
when you are not using the spark cluster as it would the compute engine cluster create would incur disk fees even when the cluster is not running