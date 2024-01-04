# Practice Data Project

This repository contains information and instructions for setting up a practice data project using data from [vnstock](https://github.com/thinh-vu/vnstock).

## Data to be Crawled:

The following data will be crawled:
- **Daily**
    - **Công ty (Companies):**
        - Danh sách công ty (Company listing)
        - Mức biến động giá cổ phiếu (Ticker price volatility)
        - Thông tin giao dịch nội bộ (Company insider deals)
        - Thông tin sự kiện quyền (Company events)
        - Tin tức công ty (Company news)
        - Giá cổ phiếu (Stock history)
        - Dữ liệu khớp lệnh trong ngày giao dịch (Stock intraday)
        - Định giá cổ phiếu (Stock evaluation)
        - Đánh giá cổ phiếu (Stock rating)
        
    - **Quỹ (Funds):**
        - Danh sách quỹ (Funds listing)
        - Các mã quỹ nắm giữ (Top holding list details)
        - Ngành mà quỹ đang đầu tư (Industry holding list details)
        - Báo cáo NAV (Nav report)
        - Tỉ trọng tài sản nắm giữ (Asset holding list)
        
- **Quarterly:**
    - Thông tin tổng quan (Company overview)
    - Hồ sơ công ty (Company profile)
    - Danh sách cổ đông (Company large shareholders)
    - Các chỉ số tài chính cơ bản (Company fundamental ratio)
    - Danh sách công ty con, công ty liên kết (Company subsidiaries listing)
    - Ban lãnh đạo công ty (Company officers)
    - Chỉ số tài chính cơ bản (Financial ratio)
    - Báo cáo kinh doanh (Income statement)
    - Bảng cân đối kế toán (Balance sheet)
    - Báo cáo lưu chuyển tiền tệ (Cash flow)

Please be advised that the functionality to capture funds is specifically accessible in `vnstock` version `0.2.8.7` and beyond. It's crucial to acknowledge that this version is not compatible with Python 3.8, the latest Python version for Airflow with Docker.

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

## Store data in HDFS with Docker

### Getting Started
1. **Clone the Repository:**
   Clone the Docker Hadoop repository from GitHub.

   ```bash
   git clone https://github.com/big-data-europe/docker-hadoop
   ```

2. **Start Hadoop Cluster:**
   Move into the cloned repository and start the Hadoop cluster using Docker Compose.

   ```bash
   cd docker-hadoop
   docker-compose up -d
   ```

   This command launches the Hadoop services in detached mode.

### Accessing the Web Interface
The Hadoop webserver is available at: [http://localhost:9870](http://localhost:9870).

### WebHDFS commands to access HDFS
The complete documentation for WebHDFS commands can be found [here](https://hadoop.apache.org/docs/r1.0.4/webhdfs.html#CREATE).

**Check HDFS Status:** 
To check the status of HDFS, initiate a PUT request to the WebHDFS endpoint.

```bash
curl -i -X PUT "http://localhost:9870/webhdfs/v1/user_data?op=CREATE"
```

*Note: If running in Airflow jobs, replace `localhost` with `host.docker.internal`.*

This command checks the HDFS status and ensures that the Hadoop cluster is running properly.

**Create a New File:**
Create a new file in HDFS by executing a PUT request with the local file.

```bash
curl -i -X PUT -T <LOCAL_FILE> "http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=CREATE&namenoderpcaddress=namenode:9000"
```

Replace `<LOCAL_FILE>`, `<DATANODE>`, `<PORT>`, and `<PATH>` with your specific details.

Example:

```bash
curl -v -i -X PUT -T ./data/outputs/output_daily_03-01-2024.xlsx "http://localhost:9864/webhdfs/v1/user_data/output_daily_03-01-2024.xlsx?op=CREATE&namenoderpcaddress=namenode:9000"
```

**Open a File:**
Open a file from HDFS using a GET request.

```bash
curl -i "http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=OPEN&namenoderpcaddress=namenode:9000&offset=0" -o <LOCAL_PATH>
```

Example:

```bash
curl -i "http://localhost:9864/webhdfs/v1/user_data/output_daily_03-01-2024.xlsx?op=OPEN&namenoderpcaddress=namenode:9000&offset=0" -o output_daily_03-01-2024.xlsx
```

*Note: If running in Airflow jobs, replace `<DATANODE>` with `localhost` or `host.docker.internal`.*

This command downloads the specified file from HDFS to your local machine.

**Show All Subdirectories:**
Display information about all subdirectories in a specific HDFS path.

```bash
curl -i "http://localhost:9870/webhdfs/v1/user_data?op=LISTSTATUS"
```

*Note: If running in Airflow jobs, replace `localhost` with `host.docker.internal`.*

This command lists the status of all subdirectories in the specified HDFS path.
