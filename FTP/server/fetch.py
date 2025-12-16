import paramiko
import os
import sys

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

# Bepaal de map van dit script, zodat we daar een logbestand kunnen wegschrijven

log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "process.log")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
with open(log_file_path, 'a', encoding='utf-8') as log_file:
    log_file.write(f"\n=== Start run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Bereken datum voor 2 maanden geleden
two_months_ago = (datetime.now() - timedelta(days=60))

# Maak een tijdelijke map aan in de parent map van de doelmap
temp_folder = os.path.join(os.path.dirname(root_dir), "temp")
if not os.path.exists(temp_folder):
    os.makedirs(temp_folder)
    print(f"Tijdelijke map gemaakt: {temp_folder}")

# =========================
# Railway ENV credentials
# =========================
private_key = paramiko.Ed25519Key.from_private_key_file("/data/ftp_key")

hostname = os.environ.get("FTP_HOST", "91.213.201.22")
username_sftp = os.environ.get("FTP_USER", "3182")
remote_dir = os.environ.get("FTP_REMOTE_DIR", "/outgoing")



# Regex-patronen voor het vinden van datums in bestandsnamen
date_patterns = [
    r"20\d{2}[01]\d[0-3]\d",  # YYYYMMDD, begint altijd met 20
    r"20\d{2}-\d{2}-\d{2}",  # YYYY-MM-DD, begint altijd met 20
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
        client.connect(hostname, username=username_sftp, pkey=private_key)
        sftp = client.open_sftp()
        sftp.chdir(remote_dir)

        created_directories = {}
        files = sftp.listdir()
        print(f"Bestanden gevonden: {files}")

        for file in files:
            try:
                print(f"Processing file: {file}")
                # Log naar ons logbestand
                with open(log_file_path, 'a', encoding='utf-8') as log_file:
                    log_file.write(f"Processing file: {file}\n")

                if file.endswith('.html.zip'):
                    isin_code = find_isin_code(file)
                    if isin_code:
                        print(f"Gevonden ISIN-code: {isin_code}")
                        with open(log_file_path, 'a', encoding='utf-8') as log_file:
                            log_file.write(f"ISIN-code gevonden in HTML: {isin_code}\n")

                        html_folder = os.path.join(root_dir, "CA Notifications", isin_code)
                        if not os.path.exists(html_folder):
                            os.makedirs(html_folder)
                            print(f"Map gemaakt: {html_folder}")
                        local_html_zip_path = os.path.join(html_folder, file)
                        sftp.get(f"{remote_dir}{file}", local_html_zip_path)
                        unzip_file(local_html_zip_path, html_folder)
                        os.remove(local_html_zip_path)
                        print(f"Zipbestand {local_html_zip_path} verwijderd.")
                    else:
                        print("Geen ISIN-code gevonden in het HTML-bestand.")
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
                            print(f"Map gemaakt: {date_folder}")

                        local_zip_path = os.path.join(temp_folder, file)
                        sftp.get(f"{remote_dir}{file}", local_zip_path)
                        unzip_file(local_zip_path, date_folder)
                        os.remove(local_zip_path)
                        print(f"Zipbestand {local_zip_path} verwijderd.")
                    else:
                        print("Geen datum gevonden in de bestandsnaam.")
                        with open(log_file_path, 'a', encoding='utf-8') as log_file:
                            log_file.write("Geen datum gevonden in bestandsnaam.\n")
            except Exception as e:
                print(f"Fout bij verwerken van bestand '{file}': {e}")
                with open(log_file_path, 'a', encoding='utf-8') as log_file:
                    log_file.write(f"Fout bij verwerken van bestand '{file}': {e}\n")

        sftp.close()
        client.close()

    except Exception as e:
        print(f"Er ging iets fout bij het verbinden met de SFTP-server: {e}")
        with open(log_file_path, 'a', encoding='utf-8') as log_file:
            log_file.write(f"Fout bij verbinden SFTP-server: {e}\n")


# Voer het proces uit
process_files()
print("Bestanden verwerkt.")
