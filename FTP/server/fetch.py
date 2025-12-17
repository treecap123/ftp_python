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

# =========================
# LOGGING
# =========================
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "process.log")

with open(log_file_path, 'a', encoding='utf-8') as log_file:
    log_file.write(f"\n=== START RUN: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")

# =========================
# TEMP MAP
# =========================
temp_folder = os.path.join(os.path.dirname(root_dir), "temp")
os.makedirs(temp_folder, exist_ok=True)

print("\n" + "=" * 80)
print("📁 TEMP MAP GEBRUIKT:")
print(f"👉 {temp_folder}")
print("=" * 80 + "\n")

# =========================
# FTP / SFTP SETUP
# =========================
key_path = "/data/ftp_key"

if not os.path.exists(key_path):
    b64_key = os.environ["FTP_PRIVATE_KEY"]
    key_bytes = base64.b64decode(b64_key)

    with open(key_path, "wb") as f:
        f.write(key_bytes)

    os.chmod(key_path, 0o600)

private_key = paramiko.RSAKey.from_private_key_file(key_path)

hostname = os.environ.get("FTP_HOST", "91.213.201.22")
username_sftp = os.environ.get("FTP_USER", "3182")
remote_dir = os.environ.get("FTP_REMOTE_DIR", "/outgoing")

print("\n" + "=" * 80)
print("🔌 SFTP VERBINDING:")
print(f"HOST      : {hostname}")
print(f"USER      : {username_sftp}")
print(f"REMOTE DIR: {remote_dir}")
print("=" * 80 + "\n")

# =========================
# HELPERS
# =========================
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

    print("✅ ZIP UITGEPAKT")
    print(f"📂 DOELMAP : {extract_to_folder}")

    extracted_files = os.listdir(extract_to_folder)
    if extracted_files:
        print("📄 BESTANDEN GEVONDEN:")
        for f in extracted_files:
            print(f"   - {f}")
    else:
        print("❌ WAARSCHUWING: MAP IS LEEG NA UNZIP")

def format_date_to_yyyy_mm_dd(date_str):
    if re.match(r"20\d{2}[01]\d[0-3]\d", date_str):
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    return date_str

def find_isin_code(filename):
    match = re.search(r'[A-Z]{2}\d{10}', filename)
    return match.group() if match else None

# =========================
# MAIN PROCESS
# =========================
def process_files():
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=hostname, username=username_sftp, pkey=private_key)

        sftp = client.open_sftp()
        sftp.chdir(remote_dir)

        files = sftp.listdir()

        print("\n" + "=" * 80)
        print("📥 BESTANDEN OP SFTP:")
        for f in files:
            print(f"   - {f}")
        print("=" * 80 + "\n")

        for file in files:
            print("\n" + "-" * 80)
            print(f"🚀 VERWERKEN VAN BESTAND: {file}")
            print("-" * 80)

            if file.endswith('.html.zip'):
                isin_code = find_isin_code(file)
                if not isin_code:
                    print("❌ GEEN ISIN GEVONDEN – SKIP")
                    continue

                html_folder = os.path.join(root_dir, "CA Notifications", isin_code)
                os.makedirs(html_folder, exist_ok=True)

                print("📁 HTML BESTAND WORDT OPGESLAGEN IN:")
                print(f"👉 {html_folder}")

                local_zip = os.path.join(html_folder, file)
                sftp.get(file, local_zip)

                print(f"⬇️ GEDOWNLOAD: {local_zip}")
                unzip_file(local_zip, html_folder)
                os.remove(local_zip)

            else:
                found_date = find_date_in_filename(file)
                if not found_date:
                    print("❌ GEEN DATUM IN BESTANDSNAAM – SKIP")
                    continue

                formatted_date = format_date_to_yyyy_mm_dd(found_date)
                date_folder = os.path.join(root_dir, formatted_date)
                os.makedirs(date_folder, exist_ok=True)

                print("📁 ZIP WORDT UITGEPAKT NAAR:")
                print(f"👉 {date_folder}")

                local_zip = os.path.join(temp_folder, file)
                sftp.get(file, local_zip)

                print(f"⬇️ GEDOWNLOAD NAAR TEMP:")
                print(f"👉 {local_zip}")

                unzip_file(local_zip, date_folder)
                os.remove(local_zip)

        sftp.close()
        client.close()

        print("\n" + "=" * 80)
        print("✅ ALLE BESTANDEN ZIJN VERWERKT")
        print("=" * 80 + "\n")

    except Exception as e:
        print("❌ FATALE FOUT:", e)

process_files()
