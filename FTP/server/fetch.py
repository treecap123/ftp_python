import paramiko
import os
import sys
import base64
import re
import zipfile
import time
from datetime import datetime
from io import StringIO

# =========================================================
# PATHS
# =========================================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, BASE_DIR)

from Functions.system.path import root_dir

# =========================================================
# LOCAL BASE64 SSH KEY
# =========================================================
LOCAL_PRIVATE_KEY_B64 = "LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFb2dJQkFBS0NBUUVBalBKdklTVDVqR0NkYlV1bkZyelJxM0d5RTF2QWtkWDdnMnUwWStKL0phVTQvdmE2Ck5oeUx4QmRsemM0R0ZBd09tQU5Ha1R6KzRheTdreHAvcmU0U0hGcWtLY3hpRjQvRkdseml2Y2liRk5WL05xSTYKcVZPVDcrOEZKcU1VeUZRVW1TRmRId0drc2MxcFBEVVIrZTJwbFlkak9pR2IrYnRwZFRaanpzazk4L1M1b0dIdwpvNXR1L0hoYk9PbHY0b2oyakcycm1FWlNoejAwWXp1dmRjN2FldkFoRW5TZWhid1JEVisvVXFXTWtsTm9jTVJBCklQdUoyUWJMRTlqbFVzdTdGMzhjc0lsLzdGZ2VZUVlKcENmN0RpVytiMk01VTFqUGNjVlpuYTE3Q0NET21yRm0KanZCMDNsQ2VZalk4T0NCTTVtR3ZmbmdYbGtyMXRCWGpaR1VOZVFJQkpRS0NBUUJ5U0FjVDlIZFBPWlJtZExEOQpyZHB2VlVSSERCK1kxd05IOW5hV0xVUi9ZMTZsTnNCKzVyYURVVDNKZHAwbEF0dG1mekpNUDBzemhTSjNSZXI3CnVoeUZzVWJWeUUrSXVjSm9adjBJbE5ERXlNZ0N5TG54RTFWYXdjelRPQjZ3UkN4Z2UrcWpoTXE1clhmcm9LYksKcFFZS2lYTU1pZlFXbC9Ta09mQVBjcnljU0taeHMvZzZId2x2b3pjL0tXUDJ4eUt3NXgrSmxuOVZqM1BFR2VvMAo1eFpCdDFScTdGd2paUjBndndqN0IvTmo0T2FrbWNRZE9GaWNwTG45cE03dWJ4N1BHU1hWWVpHdnNqVDE2dTJMCmtHNCtmaUtlNGx2RFJrTDhYbWk4U2ZwbE1YNW9lSStRYklHU0Y5WHZXeE5BcmJBWjVZc0gyQUtxT1VvVmhYMEcKeFlObEFvR0JBTzd0SGJ6dTZrZTk4TmZmTDBRYldmSmVSUUpSaTdmVExpeUNqbzlNekNsUFFKS2picng5Wm9iaApqYnVQSHNFdzRWOC82R1J3dVltOWxhNnZtejZmTkZ0bUUwRU0vM3kvQm55QkNtc25KdWc2aU9XODFMckJNMUhsCjIwNWlQaVF6OURiT1A2ZjFEUVI2S2h3TTBjNi9weE9oaWJqK09YNy9VeFJUMU81UTlYcVhBb0dCQUpjRTVqMUsKNERVMDE5QkhqNmYwc09IdGxDU0RiK3lYVlpLekZiS0JYU2JKa2JkaW5oc1A0VjIyb2lOYXVMak1xWFZEdDlBMgo0dWduREZraTRWSW5IMlJjcXJXQ3hPQ1FYRDVzYXZEN2lxeitBVWVaUkI1MnBDcnVRbWZzejlVMVBGbkRFRUM5CkdWa2U1dnA2L0tLR2tmRWs1NGJEWHRtUjAxdFd4djBDalFwdkFvR0JBTFRQSFd4aHhnekhGeC9uS3JBVXNzVkEKYTVNTlJ5TlR0RDFiNFlFek1yZDZQclFvcHRQVWhQQnpWb0FaUjdUTEMycXM1emRBakdnOGNVWHNyTlZ4ajJmQwozaU5qeUVLNkVyOFZpMWdCOCs0Q3lIYUJENi9zSnRZeFo3RHdabUNXQk9zMFM5a01lSTNFTzRyWlJOUFBUZ0VFCm41THA1bEpFclpMWHFBNUxHcUgxQW9HQWNramxsaXJUTHk3b2dmRDNGMUZqUXpCVWJxaW51Zlg3a1p4SHhWcjYKWW9xemRnVnd1b2lPM3lKczNId0JhVUR1OFBUNXpnQUZxTU9hcEZHL1JSYXYrT3d1S0g2aTAzUXFJV2JOZUJoTgpSS1NMV01jREhmanE4QTVBRjB0czJMS3FOaGNUTitrTVEzRlUyVHA1L25Pc3Rueks0b1lDbHM4NEtYSWFDNU02ClRSVUNnWUVBMmdOMlIvSjViTFlDbFd4SzdWcU4yMzRleitmQ3RqWTQ5NEE1QXR0N3JONEtzY3JDZE9GaU5JdG0KVHZnR1g3K3JuYkJqMzdlQjZJZ3FJSTZmSXMyRi94Z1hpR0dtWkY5Y3BnbFJ5UjEwY0ZaWU1FY0syaTdZUWxEZQpaelVuZFhnR25JbDhVNkloQm5PYlpxbnFwWCtreHJuUWlGOFk1UlF1YXpJZ3ZGdzBpZHM9Ci0tLS0tRU5EIFJTQSBQUklWQVRFIEtFWS0tLS0tCg=="

# =========================================================
# CONFIG
# =========================================================
SLEEP_SECONDS = 3600  # 1 uur
REMOTE_DIR = os.environ.get("FTP_REMOTE_DIR", "/outgoing")
HOSTNAME = os.environ.get("FTP_HOST", "91.213.201.22")
USERNAME = os.environ.get("FTP_USER", "3182")

TEMP_DIR = os.path.join(os.path.dirname(root_dir), "temp")
os.makedirs(TEMP_DIR, exist_ok=True)

# =========================================================
# SSH KEY LOAD
# =========================================================
key_b64 = os.environ.get("FTP_PRIVATE_KEY", LOCAL_PRIVATE_KEY_B64)
private_key_str = base64.b64decode(key_b64).decode("utf-8")
PRIVATE_KEY = paramiko.RSAKey.from_private_key(StringIO(private_key_str))

# =========================================================
# HELPERS
# =========================================================
DATE_PATTERNS = [r"20\d{2}[01]\d[0-3]\d", r"20\d{2}-\d{2}-\d{2}"]

def find_date(filename):
    for p in DATE_PATTERNS:
        m = re.search(p, filename)
        if m:
            return m.group()
    return None

def normalize_date(date_str):
    if len(date_str) == 8:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    return date_str

def find_isin(filename):
    m = re.search(r"[A-Z]{2}\d{10}", filename)
    return m.group() if m else None

def unzip(zip_path, target):
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(target)

# =========================================================
# MAIN LOOP
# =========================================================
print("🚀 SFTP SERVICE STARTED (HOURLY MODE)")

while True:
    print(f"\n⏰ RUN @ {datetime.now()}")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=HOSTNAME,
        username=USERNAME,
        pkey=PRIVATE_KEY,
        timeout=15
    )

    sftp = client.open_sftp()
    sftp.chdir(REMOTE_DIR)

    remote_files = sftp.listdir()

    for file in remote_files:
        print(f"🔍 CHECK: {file}")

        # -----------------------------
        # HTML ZIP (ISIN based)
        # -----------------------------
        if file.endswith(".html.zip"):
            isin = find_isin(file)
            if not isin:
                print("❌ no ISIN → skip")
                continue

            target_dir = os.path.join(root_dir, "CA Notifications", isin)
            os.makedirs(target_dir, exist_ok=True)

        # -----------------------------
        # DATE ZIP
        # -----------------------------
        else:
            date = find_date(file)
            if not date:
                print("❌ no date → skip")
                continue

            target_dir = os.path.join(root_dir, normalize_date(date))
            os.makedirs(target_dir, exist_ok=True)

        # -----------------------------
        # DUPLICATE CHECK
        # -----------------------------
        done_marker = os.path.join(target_dir, f".{file}.done")

        if os.path.exists(done_marker):
            print("⏭️  ALREADY PROCESSED")
            continue

        # -----------------------------
        # DOWNLOAD
        # -----------------------------
        local_zip = os.path.join(TEMP_DIR, file)
        print(f"⬇️ DOWNLOAD {file}")
        sftp.get(file, local_zip)

        # -----------------------------
        # UNZIP
        # -----------------------------
        unzip(local_zip, target_dir)
        os.remove(local_zip)

        # -----------------------------
        # MARK AS DONE
        # -----------------------------
        with open(done_marker, "w") as f:
            f.write(datetime.now().isoformat())

        print("✅ DONE")

    sftp.close()
    client.close()

    print("😴 SLEEPING 1 HOUR...\n")
    time.sleep(SLEEP_SECONDS)
