import subprocess
from setup_logger import create_logger

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


logger = create_logger("../data/")
