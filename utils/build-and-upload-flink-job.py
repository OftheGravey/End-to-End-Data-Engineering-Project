import os
import subprocess
import requests
import json 

FLINK_PROJECT_DIR = r".\flink-extractor"
FLINK_JOBMANAGER_URL = "http://localhost:8081"
EXPECTED_JAR_FILE_NAME = "flink-extractor-1.0-SNAPSHOT.jar"
TARGET_DIR = os.path.join(FLINK_PROJECT_DIR, "target")
LOCAL_JAR_PATH = os.path.join(TARGET_DIR, EXPECTED_JAR_FILE_NAME)

def build_flink_project(project_dir):
    print(f"Navigating to project directory: {project_dir}")
    try:
        os.chdir(project_dir)
    except FileNotFoundError:
        print(f"Error: Project directory not found at {project_dir}")
        return False

    print("Building Flink project with Maven...")
    try:
        maven_command = ["mvn", "clean", "package", "-DskipTests"]
        result = subprocess.run(maven_command, capture_output=True, text=True, check=False, shell=True)

        print(result.stdout)
        if result.stderr:
            print(f"Maven stderr:\n{result.stderr}")

        if result.returncode == 0:
            print("Maven build successful.")
            os.chdir('..')
            return True
        else:
            print(f"Maven build failed. Exit Code: {result.returncode}")
            return False
    except Exception as e:
        print(f"An error occurred during Maven build: {e}")
        return False

def upload_flink_jar(flink_url, jar_path):
    if not os.path.exists(jar_path):
        print(f"Error: JAR file not found at {jar_path}. Aborting upload.")
        return None

    upload_endpoint = f"{flink_url}/jars/upload"
    print(f"Uploading JAR '{jar_path}' to Flink at {upload_endpoint}")

    try:
        with open(jar_path, 'rb') as f:
            files = {'jarfile': (os.path.basename(jar_path), f, 'application/java-archive')}
            response = requests.post(upload_endpoint, files=files)
            response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)

            response_data = response.json()
            if response_data and 'filename' in response_data:
                uploaded_filename = response_data['filename']
                print(f"JAR uploaded successfully. Flink filename: {uploaded_filename}")
                return uploaded_filename
            else:
                print(f"Failed to upload JAR. Flink API response: {json.dumps(response_data, indent=2)}")
                return None
    except requests.exceptions.RequestException as e:
        print(f"Error during JAR upload: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Flink API Error Response: {e.response.text}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during JAR upload: {e}")
        return None

if __name__ == "__main__":
    print("Starting Flink job build and upload process...")
    if build_flink_project(FLINK_PROJECT_DIR):
        print(f"Build complete. Looking for JAR at: {LOCAL_JAR_PATH}")
        uploaded_jar_id = upload_flink_jar(FLINK_JOBMANAGER_URL, LOCAL_JAR_PATH)

        if uploaded_jar_id:
            print(f"\nSuccessfully built and uploaded Flink JAR '{EXPECTED_JAR_FILE_NAME}'.")
        else:
            print("\nJAR upload failed. See errors above.")
    else:
        print("\nFlink project build failed. Aborting.")