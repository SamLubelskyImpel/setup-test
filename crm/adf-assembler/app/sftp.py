import paramiko
import logging
from os import environ

logger = logging.getLogger()
logger.setLevel(logging.getLevelName(environ.get("LOGLEVEL", "INFO").upper()))


def put_adf(sftp_config: dict, adf: str, filename: str):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect(
        hostname=sftp_config.get("host"),
        port=sftp_config.get("port", 22),
        username=sftp_config.get("username"),
        password=sftp_config.get("password")
    )

    sftp = ssh.open_sftp()
    remote_location = f'{sftp_config.get("location", "")}/{filename}'
    remote_file = sftp.open(remote_location, mode='w')
    remote_file.write(adf)

    logger.info(f"[sftp] ADF file uploaded to {remote_location}")

    remote_file.close()
    sftp.close()
    ssh.close()
