import paramiko
import os
import sys
import base64
sys.path.append('/utility')
import re
import tempfile
import io
import zipfile
from datetime import datetime, timedelta
import subprocess

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, BASE_DIR)

from Functions.system.path import root_dir, base_dir

# Logbestand
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "process.log")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
with open(log_file_path, 'a', encoding='utf-8') as log_file:
    log_file.write(f"\n=== Start run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Datum 2 maanden geleden
two_months_ago = (datetime.now() - timedelta(days=60))

# Temp map
temp_folder = os.path.join(os.path.dirname(root_dir), "temp")
if not os.path.exists(temp_folder):
    os.makedirs(temp_folder)
    print(f"Tijdelijke map gemaakt: {temp_folder}")

# =========================
# FTP / Railway credentials
# =========================

key_path = "/data/ftp_key"

if not os.path.exists(key_path):
    b64_key = os.environ["FTP_PRIVATE_KEY"]
    key_bytes = base64.b64decode(b64_key)

    with open(key_path, "wb") as f:
        f.write(key_bytes)

    os.chmod(key_path, 0o600)

# >>> DIT MISSTE BIJ JOU <<<
private_key = paramiko.RSAKey.from_private_key_file(key_path)

hostname = os.environ.get("FTP_HOST", "91.213.201.22")
username_sftp = os.environ.get("FTP_USER", "3182")
remote_dir = os.environ.get("FTP_REMOTE_DIR", "/outgoing")

# Regex voor datums
date_patterns = [
    r"20\d{2}[01]\d[0-3]\d",
    r"20\d{2}-\d{2}-\d{2}",
]

def find_date_in_filename(filename):
    for pattern in date_patterns:
        match = re.search(pattern, filename)
        if match:
            return match.group()
    return None

def unzip_file(zip_filepath, extract_to_folder):
    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        zip_ref.extractall(extract_to_folder)
    print(f"Bestand uitgepakt naar {extract_to_folder}")

def format_date_to_yyyy_mm_dd(date_str):
    if re.match(r"20\d{2}[01]\d[0-3]\d", date_str):
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    elif re.match(r"20\d{2}-\d{2}-\d{2}", date_str):
        return date_str
    else:
        raise ValueError(f"Ongeldig datumformaat: {date_str}")

def find_isin_code(filename):
    match = re.search(r'[A-Z]{2}\d{10}', filename)
    if match:
        return match.group()
    return None

def process_files():
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=hostname,
            username=username_sftp,
            pkey=private_key
        )

        sftp = client.open_sftp()
        sftp.chdir(remote_dir)

        created_directories = {}
        files = sftp.listdir()
        print(f"Bestanden gevonden: {files}")

        for file in files:
            try:
                print(f"Processing file: {file}")
                with open(log_file_path, 'a', encoding='utf-8') as log_file:
                    log_file.write(f"Processing file: {file}\n")

                if file.endswith('.html.zip'):
                    isin_code = find_isin_code(file)
                    if isin_code:
                        html_folder = os.path.join(root_dir, "CA Notifications", isin_code)
                        os.makedirs(html_folder, exist_ok=True)

                        local_html_zip_path = os.path.join(html_folder, file)
                        sftp.get(file, local_html_zip_path)

                        unzip_file(local_html_zip_path, html_folder)
                        os.remove(local_html_zip_path)
                    else:
                        with open(log_file_path, 'a', encoding='utf-8') as log_file:
                            log_file.write("Geen ISIN-code gevonden in HTML-bestand.\n")
                else:
                    found_date = find_date_in_filename(file)
                    if found_date:
                        formatted_date = format_date_to_yyyy_mm_dd(found_date)
                        date_folder = os.path.join(root_dir, formatted_date)

                        if formatted_date not in created_directories:
                            os.makedirs(date_folder, exist_ok=True)
                            created_directories[formatted_date] = True

                        local_zip_path = os.path.join(temp_folder, file)
                        sftp.get(file, local_zip_path)

                        unzip_file(local_zip_path, date_folder)
                        os.remove(local_zip_path)
                    else:
                        with open(log_file_path, 'a', encoding='utf-8') as log_file:
                            log_file.write("Geen datum gevonden in bestandsnaam.\n")

            except Exception as e:
                with open(log_file_path, 'a', encoding='utf-8') as log_file:
                    log_file.write(f"Fout bij verwerken van bestand '{file}': {e}\n")

        sftp.close()
        client.close()

    except Exception as e:
        with open(log_file_path, 'a', encoding='utf-8') as log_file:
            log_file.write(f"Fout bij verbinden SFTP-server: {e}\n")

process_files()
print("Bestanden verwerkt.")
