import subprocess
import json
import os
from datetime import datetime
import pandas as pd
import openpyxl
import re

def run_curl_command(curl_command):
    try:
        result = subprocess.run(curl_command, shell=True, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running the curl command. Exit code: {e.returncode}")
        print("Error output:")
        print(e.stderr)
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
        return []

def get_file_names_with_today_date(file_names):
    today_date_str = datetime.now().strftime("%d-%m-%Y")
    return [file_name for file_name in file_names if file_name.endswith(today_date_str)]

def get_file_names():
    curl_command = 'curl -i "http://host.docker.internal:9870/webhdfs/v1/crawl_dir?op=LISTSTATUS"'

    response = run_curl_command(curl_command)

    if response is not None:
        json_data = extract_json_from_response(response)

        if json_data is not None:
            response_dict = json.loads(json_data)

            file_names = extract_file_names(response_dict)

            return file_names

def convert_excel_to_excel(file_path):
    wb = openpyxl.load_workbook(file_path)
    wb.save(file_path)

def download_files(output_folder='tmp'):
    file_names = get_file_names()

    os.makedirs(output_folder, exist_ok=True)

    base_url = "http://host.docker.internal:9864/webhdfs/v1/crawl_dir/"

    for file_name in file_names:
        file_url = f"{base_url}{file_name}?op=OPEN&namenoderpcaddress=namenode:9000&offset=0"
        output_file_path = os.path.join(output_folder, f'{file_name}')

        curl_command = f'curl -i "{file_url}" -o "{output_file_path}"'
        run_curl_command(curl_command)
        convert_excel_to_excel(output_file_path)

def convert_excel_to_csv(input_folder = "tmp" ):
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".xlsx"):  
            excel_file_path = os.path.join(input_folder, file_name)

            date_part = file_name.split("_")[-1].split(".xlsx")[0]

            excel_data = pd.read_excel(excel_file_path, sheet_name=None)

            for sheet_name, sheet_data in excel_data.items():
                if 'Summary' not in sheet_name:
                    sheet_name = sheet_name.lower().replace(" ", "_")
                    csv_file_path = os.path.join(input_folder, f"{sheet_name}_{date_part}.csv")
                    sheet_data.to_csv(csv_file_path, index=False)

def extract_csv_filename(csv_filename):
    if 'capture_' in csv_filename:
        csv_filename = csv_filename.replace('capture_', '')
    match = re.search(r'^(.+?)_[0-9]', csv_filename)
    return match.group(1).replace('_', ' ').capitalize().strip() if match else ''

def find_path_for_csv(csv_filename):
    FOLDER_STRUCTURE = [
        {
            'Company Data': [
                {'General Data': [
                    'Company listing',
                    'Company insider deals',
                    'Company events',
                    'Company news',
                    'Company overview',
                    'Company profile',
                    'Company shareholders',
                    'Fundamental ratio',
                    'Subsidiaries listing',
                    'Company officers'
                ]},
                {'Financial Data': [
                    'Financial ratio',
                    'Income statement',
                    'Balance sheet',
                    'Cash flow'
                ]}
            ]
        },
        {
            'Stock Data': [
                'Ticker volatility',
                'Stock history',
                'Stock intraday',
                'Stock evaluation',
                'Stock rating'
            ]
        },
        {
            'Fund Data': [
                'Funds listing',
                'Top holding list',
                'Industry holding list',
                'Nav report',
                'Asset holding list'        
            ]
        }
    ]   
    csv_filename = extract_csv_filename(csv_filename)
    for item in FOLDER_STRUCTURE:
        path = find_path_for_csv_helper(item, '', csv_filename)
        if path:
            return path

def find_path_for_csv_helper(substructure, current_path, csv_filename):
    if isinstance(substructure, dict):
        for key, value in substructure.items():
            new_path = f"{current_path}{key}/"
            path = find_path_for_csv_helper(value, new_path, csv_filename)
            if path:
                return path
    elif isinstance(substructure, list):
        for item in substructure:
            path = find_path_for_csv_helper(item, current_path, csv_filename)
            if path:
                return path
    else:
        if substructure == csv_filename:
            return current_path.lower().replace(" ", "_")
        else: 
            return None
    
def extract_data(source_directory = 'tmp', **kwargs):
    download_files(source_directory)
    convert_excel_to_csv(source_directory)

    bash_command = ''
    for filename in os.listdir(source_directory):
        if filename.endswith(".csv"):
            full_path = os.path.join(source_directory, filename)
            dir_path = find_path_for_csv(filename)
            full_storage_path = os.path.join(dir_path, filename) if dir_path else ''
            curl_command = f'curl -v -i -X PUT -T {full_path} "http://host.docker.internal:9864/webhdfs/v1/extract_dir/{full_storage_path}?op=CREATE&namenoderpcaddress=namenode:9000&createflag=&createparent=true&overwrite=false"\n'
            bash_command += curl_command

    kwargs['ti'].xcom_push(key='bash_command', value=bash_command)