To run locally, you need:

1. [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
2. [Github account](https://github.com/)
3. [Docker](https://docs.docker.com/engine/install/) with at least 4GB of RAM and [Docker Compose](https://docs.docker.com/compose/install/) v1.27.0 or later

Clone the repo and run the following commands to start the data pipeline:

```bash
git clone https://github.com/majay777/Coipcap_crypto_data.git
cd Coincap_crypto_data
make build
sleep 30
make up
sleep 30 # wait for Airflow to start

```

Go to [http:localhost:8080] to see the Airflow UI. Username and password are 'airflow' only.

Go to [http:localhost:9001] to minio bucket, minio user and password both are 'minioadmin'.

To run dash app:

```bash
make minio-start
sleep 30
make dash-app
```