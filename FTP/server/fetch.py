import paramiko
import os
import sys
import base64
import re
import zipfile
import time
import subprocess
from datetime import datetime
from io import StringIO
import shutil

# =========================================================
# PATHS
# =========================================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, BASE_DIR)

from Functions.system.path.path import root_dir, dropbox_path

# =========================================================
# CONFIG
# =========================================================
SLEEP_SECONDS = 3600
REMOTE_DIR = os.environ.get("FTP_REMOTE_DIR", "/outgoing")
HOSTNAME = os.environ.get("FTP_HOST", "91.213.201.22")
USERNAME = os.environ.get("FTP_USER", "3182")

TEMP_DIR = os.path.join(dropbox_path, "temp")
os.makedirs(TEMP_DIR, exist_ok=True)

# =========================================================
# SSH KEY
# =========================================================
key_b64 = os.environ.get("FTP_PRIVATE_KEY")

private_key_str = base64.b64decode(key_b64).decode("utf-8")
PRIVATE_KEY = paramiko.RSAKey.from_private_key(StringIO(private_key_str))

# =========================================================
# HELPERS
# =========================================================
def cleanup_old_folders(base_dir, keep=5):

    folders = []

    for name in os.listdir(base_dir):

        path = os.path.join(base_dir, name)

        if os.path.isdir(path):
            try:
                d = datetime.strptime(name, "%Y-%m-%d")
                folders.append((d, path))
            except:
                pass

    folders.sort(reverse=True)

    for d, path in folders[keep:]:
        print("🗑 removing", path)
        shutil.rmtree(path)


def find_date(filename):

    m = re.search(r"20\d{2}-?\d{2}-?\d{2}", filename)

    if not m:
        return None

    d = m.group().replace("-", "")

    return f"{d[:4]}-{d[4:6]}-{d[6:]}"


def find_isin(filename):

    m = re.search(r"[A-Z]{2}\d{10}", filename)

    return m.group() if m else None


def unzip(zip_path, target):

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(target)

# =========================================================
# MAIN LOOP
# =========================================================

print("🚀 SFTP SERVICE STARTED")

while True:

    print("⏰ RUN @", datetime.now())

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client.connect(
        hostname=HOSTNAME,
        username=USERNAME,
        pkey=PRIVATE_KEY,
        timeout=20
    )

    sftp = client.open_sftp()
    sftp.chdir(REMOTE_DIR)

    remote_files = sftp.listdir()

    print("📦 files:", len(remote_files))

    for file in remote_files:

        print("🔍 CHECK:", file)

        # ===============================
        # CA files
        # ===============================

        if file.endswith(".html.zip"):

            isin = find_isin(file)

            if not isin:
                continue

            target_dir = os.path.join(dropbox_path, "CA Notifications", isin)

        # ===============================
        # DATE FILES
        # ===============================

        else:

            date = find_date(file)

            if not date:
                continue

            target_dir = os.path.join(dropbox_path, date)

        os.makedirs(target_dir, exist_ok=True)

        done_marker = os.path.join(target_dir, f".{file}.done")

        if os.path.exists(done_marker):
            continue

        local_file = os.path.join(TEMP_DIR, file)

        print("⬇ DOWNLOAD", file)

        try:
            sftp.get(file, local_file)
        except Exception as e:
            print("❌ download failed", e)
            continue

        try:

            if file.endswith(".zip"):
                unzip(local_file, target_dir)

            else:
                shutil.move(local_file, os.path.join(target_dir, file))

            with open(done_marker, "w") as f:
                f.write(datetime.now().isoformat())

            print("✅ DONE", file)

        except Exception as e:
            print("❌ processing failed", e)

        if os.path.exists(local_file):
            os.remove(local_file)

    cleanup_old_folders(dropbox_path, keep=5)

    sftp.close()
    client.close()

    # ==================================
    # RUN IMPORT SCRIPTS
    # ==================================

    imports_dir = os.path.join(root_dir, "single_imports")

    print("📂 looking for imports:", imports_dir)

    if os.path.exists(imports_dir):

        for file in os.listdir(imports_dir):

            if file.endswith(".py"):

                script = os.path.join(imports_dir, file)

                print("🚀 RUNNING:", file)

                subprocess.run(["python", script])

    print("😴 sleep 1 hour")

    time.sleep(SLEEP_SECONDS)