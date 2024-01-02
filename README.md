# Practice Data Project

This repository contains information and instructions for setting up a practice data project using data from [vnstock](https://github.com/thinh-vu/vnstock). The primary focus is on automating tasks using Apache Airflow.

## Automation with Airflow
This project leverages the Docker environment for executing Apache Airflow. For comprehensive guidance, consult the detailed documentation available at [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

### Prerequisites
1. Install [Docker Community Edition (CE)](https://docs.docker.com/engine/install/) on your workstation. Configure Docker to use at least 4.00 GB of memory for the Airflow containers to run properly. Refer to the Resources section in the Docker for Windows or Docker for Mac documentation for more information.
2. Install [Docker Compose v2.14.0](https://docs.docker.com/compose/install/) or newer on your workstation.

### Getting Started
1. Fetch the docker-compose.yaml file:
    ```bash
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.8.0/docker-compose.yaml'
    ```
2. For MacOS, if you encounter the "docker-credential-osxkeychain" error, check your `~/.docker/config.json` and replace "credsStore" with "credStore".
3. Initialize the environment:
    ```bash
    mkdir -p ./dags ./logs ./plugins ./config
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```
    For MacOS and Windows operating systems, you may get a warning that AIRFLOW_UID is not set. You can safely ignore it or manually create an .env file in the same folder as docker-compose.yaml with the content:
    ```env
    AIRFLOW_UID=50000
    ```
4. Initialize the database:
    ```bash
    docker compose up airflow-init
    ```
    Once the initialization is complete, you should observe a message similar to the following:
    ```bash
    airflow-init_1 | Upgrades done
    airflow-init_1 | Admin user 'airflow' created
    airflow-init_1 | Apache Airflow 2.8.0
    start_airflow-init_1 exited with code 0
    ```

### Special Case - Adding Dependencies via requirements.txt File
1. Comment out the image line and remove the comment from the build line in the docker-compose.yaml file.
    ```yaml
    #image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.8.1}
    build: .
    ```
2. Create Dockerfile in the same folder as your docker-compose.yaml file:
    ```Dockerfile
    FROM apache/airflow:2.8.0
    ADD requirements.txt .
    RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
    ```
3. Place requirements.txt file in the same directory.

### Running Airflow
Run the following command to start Airflow:
```bash
docker compose up
```

To access the Airflow CLI, for example, to run `airflow info`, run the following command:
```bash
docker compose run airflow-worker airflow info
```

### Accessing the Web Interface
The Airflow webserver is available at: [http://localhost:8080](http://localhost:8080). Use the default account with login `airflow` and password `airflow`.

### Cleaning Up
To stop and delete containers, delete volumes with database data, and download images, run:
```bash
docker compose down --volumes --rmi all
```

Feel free to explore and practice with the provided data using this automated Apache Airflow setup. For any additional information, refer to the linked documentation.