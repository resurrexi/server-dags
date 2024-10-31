# Server DAGs

Airflow DAGs for homelab server

## Setup and first run

1. Make sure to be in the project directory and create `.env` file.

```sh
touch .env
```

2. Create env variables

```sh
echo -e "AIRFLOW_UID=$(id -u)" >> .env
echo -e "AIRFLOW_VERSION=2.10.2" >> .env
echo -e "SHARE_DIR=/home/${USER}/Videos" >> .env
```

3. Create folders for FastAnime

```sh
mkdir -p .cache/FastAnime
mkdir -p .config/fastanime
```

4. Build docker image

```sh
docker compose build
```

5. Run docker image

```sh
docker compose up airflow-init && docker oompose up -d
```
