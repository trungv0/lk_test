# LK test

## Setup airflow locally using docker compose

For initialization:

```shell
docker-compose up airflow-init
```

Start airflow:

```shell
docker-compose up
```

For more details, visit [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html).

## Set connections

* `airtable_default`:
  * conn type: `http`
  * host: `https://api.airtable.com/v0`
  * password: use API key for Airtable
* `s3_default`:
  * conn type: `s3`
  * login: use access key
  * password: use secret key
  * extra: `{"region_name": "eu-west-1"}`
* `redshift_default`:
  * conn type: `postgres`
  * host, login, password, port as needed
  * schema: use **database name**
