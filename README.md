# LK test

## Setup airflow locally using docker compose

```shell
docker-compose up
```

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
  * host, schema, login, password, port
