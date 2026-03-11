import os

# Railway working directory
root_root_dir = os.getcwd()

# Backend root
root_dir = os.path.join(root_root_dir, "FTP")

# Data volume (SFTP downloads)
dropbox_path = "/data"

# env file
env_path = os.path.join(root_root_dir, ".env")

print("Running on Railway")
print("Data path:", dropbox_path)