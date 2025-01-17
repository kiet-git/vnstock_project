import subprocess
import json
import os
from datetime import datetime
import pandas as pd
from modules.setup_logger import create_logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, date_format, col

def run_curl_command(curl_command):
    try:
        result = subprocess.run(curl_command, shell=True, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running the curl command. Exit code: {e.returncode}")
        print("Error output:")
        print(e.stderr)
        logger.error(f"Error running the curl command. Exit code: {e.returncode}")
        logger.error(e.stderr)
        return None

def extract_json_from_response(response):
    start_index = response.find('{')
    return response[start_index:] if start_index != -1 else None

def extract_file_names(response_dict):
    try:
        file_names = [file['pathSuffix'] for file in response_dict['FileStatuses']['FileStatus']]
        return file_names
    except KeyError as e:
        print(f"Error extracting file names. Key not found: {e}")
        logger.error(f"Error extracting file names. Key not found: {e}")
        return []

def get_file_names_with_today_date(file_names):
    today_date_str = datetime.now().strftime("%d-%m-%Y")
    result = [file_name for file_name in file_names if file_name.endswith(f'{today_date_str}.xlsx')]
    logger.info(f"Retrieved {today_date_str}'s file names: {result}")
    return result

def get_file_names():
    curl_command = f'curl -i "http://host.docker.internal:9870/webhdfs/v1/crawl_dir?op=LISTSTATUS"'

    response = run_curl_command(curl_command)

    if response is not None:
        json_data = extract_json_from_response(response)

        if json_data is not None:
            response_dict = json.loads(json_data)

            file_names = extract_file_names(response_dict)
            
            logger.info(f"Retrieved file names: {file_names}")
            return file_names
        
def download_files(output_folder='tmp'):
    file_names = get_file_names_with_today_date(get_file_names())

    os.makedirs(output_folder, exist_ok=True)

    base_url = f"http://host.docker.internal:9864/webhdfs/v1/crawl_dir/"

    for file_name in file_names:
        file_url = f"{base_url}{file_name}?op=OPEN&namenoderpcaddress=namenode:9000&offset=0"
        output_file_path = os.path.join(output_folder, f'{file_name}')

        curl_command = f'curl "{file_url}" -o "{output_file_path}"'
        run_curl_command(curl_command)
   
def convert_excel_to_parquet(input_folder="tmp"):

    spark = SparkSession.builder.appName("ExcelToParquet").getOrCreate()

    for file_name in os.listdir(input_folder):
        if file_name.endswith(".xlsx"):
            excel_file_path = os.path.join(input_folder, file_name)

            date_str = file_name.split("_")[-1].split(".xlsx")[0]

            excel_data = pd.read_excel(excel_file_path, sheet_name=None)

            for sheet_name, sheet_df in excel_data.items():
                if 'Summary' not in sheet_name:
                    sheet_name = sheet_name.lower().replace(" ", "_")
                    if 'capture_' in sheet_name:
                        sheet_name = sheet_name.replace('capture_', '')

                    date_obj = datetime.strptime(date_str, "%d-%m-%Y").date()
                    spark_df = spark.createDataFrame(sheet_df)
                    spark_df = spark_df.withColumn("date", lit(date_obj))
                    spark_df = spark_df.withColumn("date", date_format(col("date"), "dd-MM-yyyy"))
                    parquet_file_path = os.path.join(input_folder, f"{sheet_name}")
                    if os.path.exists(parquet_file_path):
                        mode = "append"
                    else:
                        mode = "overwrite"
                    spark_df.write.partitionBy("date").parquet(parquet_file_path, mode=mode)

    spark.stop()

# def convert_excel_to_parquet(input_folder="tmp"):
#     for file_name in os.listdir(input_folder):
#         if file_name.endswith(".xlsx"):
#             excel_file_path = os.path.join(input_folder, file_name)

#             date_part = file_name.split("_")[-1].split(".xlsx")[0]

#             excel_data = pd.read_excel(excel_file_path, sheet_name=None)

#             for sheet_name, sheet_data in excel_data.items():
#                 if 'Summary' not in sheet_name:
#                     sheet_name = sheet_name.lower().replace(" ", "_")
#                     parquet_file_path = os.path.join(input_folder, f"{sheet_name}_{date_part}.parquet")
#                     fp.write(parquet_file_path, sheet_data)

# def convert_excel_to_csv(input_folder = "tmp" ):
#     for file_name in os.listdir(input_folder):
#         if file_name.endswith(".xlsx"):  
#             excel_file_path = os.path.join(input_folder, file_name)

#             date_part = file_name.split("_")[-1].split(".xlsx")[0]

#             excel_data = pd.read_excel(excel_file_path, sheet_name=None)

#             for sheet_name, sheet_data in excel_data.items():
#                 if 'Summary' not in sheet_name:
#                     sheet_name = sheet_name.lower().replace(" ", "_")
#                     csv_file_path = os.path.join(input_folder, f"{sheet_name}_{date_part}.csv")
#                     sheet_data.to_csv(csv_file_path, index=False)

# def extract_filename(filename):
#     if 'capture_' in filename:
#         filename = filename.replace('capture_', '')
#     match = re.search(r'^(.+?)_[0-9]', filename)
#     return match.group(1).replace('_', ' ').capitalize().strip() if match else ''

def find_path(dirname):
    from modules.extract_folder_struct import FOLDER_STRUCTURE
    for item in FOLDER_STRUCTURE:
        path = find_path_helper(item, '', dirname)
        if path:
            return path

def find_path_helper(substructure, current_path, dirname):
    if isinstance(substructure, dict):
        for key, value in substructure.items():
            new_path = f"{current_path}{key}/"
            path = find_path_helper(value, new_path, dirname)
            if path:
                return path
    elif isinstance(substructure, list):
        for item in substructure:
            path = find_path_helper(item, current_path, dirname)
            if path:
                return path
    else:
        if substructure.lower() == dirname.lower():
            return f'{current_path}{dirname}/'.lower().replace(" ", "_")
        else: 
            return None

logger = create_logger()
def extract_data(source_directory = 'tmp', **kwargs):
    logger.info("Extract data started")
    logger.info("Download file from HDFS started")
    download_files(source_directory)
    logger.info("Download file from HDFS ended")

    logger.info("Convert file from excel to parquet started")
    convert_excel_to_parquet(source_directory)
    logger.info("Convert file from excel to parquet ended")

    bash_command = ''
    for file in os.listdir(source_directory):
        file_path = os.path.join(source_directory, file)
        if os.path.isdir(file_path):
            for sub_file in os.listdir(file_path):
                sub_file_path = os.path.join(file_path, sub_file)
                if os.path.isdir(sub_file_path):
                    dir_path = find_path(file.replace("_", " ").capitalize())
                    dir_path += f"{sub_file}/"
                    for file1 in os.listdir(sub_file_path):
                        full_path = os.path.join(sub_file_path, file1)
                        full_storage_path = os.path.join(dir_path, file1) if dir_path else ''
                        curl_command = f'curl -v -i -X PUT -T {full_path} "http://host.docker.internal:9864/webhdfs/v1/extract_dir/{full_storage_path}?op=CREATE&namenoderpcaddress=namenode:9000&createflag=&createparent=true&overwrite=false"\n'
                        bash_command += curl_command

    kwargs['ti'].xcom_push(key='bash_command', value=bash_command)
    logger.info("Extract data ended")
    logger.info(f"Return bash command: {bash_command}")