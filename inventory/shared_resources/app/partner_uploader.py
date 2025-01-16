from rds_instance import RDSInstance
import logging
from os import environ
import paramiko
from json import loads
import boto3
from io import BytesIO
from typing import List
from pandas import DataFrame
import pandas as pd
from datetime import datetime, timezone
from rds_instance import RDSInstance

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ["ENVIRONMENT"]
MERCH_SFTP_KEY = environ["MERCH_SFTP_KEY"]
SALESAI_SFTP_KEY = environ["SALESAI_SFTP_KEY"]

sm_client = boto3.client("secretsmanager")


class BaseUploader:

    def __init__(self, provider_dealer_id, icc_formatted_inventory: DataFrame, config: dict):
        self.provider_dealer_id = provider_dealer_id
        self.icc_formatted_inventory = icc_formatted_inventory
        self.config = config

    def upload(self):
        raise Exception('Not implemented on base class')

    def log_info(self, msg: str):
        logger.info(f'[{self.__class__.__name__}] {msg}')


class MerchSalesAIBaseUploader(BaseUploader):

    def get_sftp_secrets(self, secret_name, secret_key):
        """Get SFTP secret from Secrets Manager."""
        secret = sm_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
        )
        secret = loads(secret["SecretString"])[str(secret_key)]
        secret_data = loads(secret)

        return secret_data["hostname"], secret_data["port"], secret_data["username"], secret_data["password"]

    def connect_sftp_server(self, hostname, port, username, password):
        """Connect to SFTP server and return the connection."""
        transport = paramiko.Transport((hostname, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        return sftp

    def proccess_and_upload_to_sftp(self, product_dealer_id, secret_key) -> None:
        """Upload to sftp server."""
        # Set DealerId to match product expected DealerId
        self.icc_formatted_inventory["DealerId"] = product_dealer_id

        csv_content = self.icc_formatted_inventory.to_csv(index=False)

        # Upload to SFTP
        hostname, port, username, password = self.get_sftp_secrets("inventory-integrations-sftp", secret_key)
        prefix = '' if ENVIRONMENT == 'prod' else 'deleteme_'
        filename = f"{prefix}{product_dealer_id}.csv"

        with self.connect_sftp_server(hostname, port, username, password) as sftp:
            csv_file_like = BytesIO(csv_content.encode())
            sftp.putfo(csv_file_like, filename)

        self.log_info(f"File {filename} uploaded to SFTP")

    def upload(self):
        dealer_id = self.config.get('dealer_id')
        sftp_key = self.config.get('sftp_key')
        self.proccess_and_upload_to_sftp(dealer_id, sftp_key)


class MerchUploader(MerchSalesAIBaseUploader):
    """Uploader for Merchandising."""


class SalesAIUploader(MerchSalesAIBaseUploader):
    """Uploader for Sales AI."""
    def proccess_and_upload_to_sftp(self, product_dealer_id, secret_key) -> None:
        """Upload to sftp server."""
        # Transpose VIN and Stock values if VIN is blank and Stock is not
        for index, row in self.icc_formatted_inventory.iterrows():
            if (pd.isna(row['VIN']) or row['VIN'] == '') and not pd.isna(row['Stock']):
                self.icc_formatted_inventory.at[index, 'VIN'] = row['Stock']

        super().proccess_and_upload_to_sftp(product_dealer_id, secret_key)


class SeezUploader(BaseUploader):
    """Uploader for Seez."""

    def get_s3_client(self):
        """Get S3 client for Seez."""
        secret = sm_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/inventory-integrations-seez"
        )
        secret = loads(secret["SecretString"])["ICC_BUCKET"]
        secret_data = loads(secret)

        s3_client = boto3.client("s3",
                            region_name=secret_data["region"],
                            aws_secret_access_key=secret_data["secret_access_key"],
                            aws_access_key_id=secret_data["access_key_id"])

        return secret_data["bucket"], s3_client

    def upload(self):
        csv_content = self.icc_formatted_inventory.to_csv(index=False)

        bucket, s3_client = self.get_s3_client()
        s3_key = f'{self.provider_dealer_id}_{datetime.now(timezone.utc).isoformat()}.csv'
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_content
        )

        self.log_info(f'Uploaded {s3_key} to Seez bucket')


SUPPORTED_UPLOADERS = {
    'seez': SeezUploader,
    'merch': MerchUploader,
    'salesai': SalesAIUploader
}


def get_partner_uploaders(provider_dealer_id, icc_formatted_inventory) -> List[BaseUploader]:
    rds_instance = RDSInstance()
    dip_metadata = rds_instance.select_db_dip_metadata(provider_dealer_id)
    if not dip_metadata:
        raise Exception(f'No metadata found for {provider_dealer_id}')

    config = dip_metadata.get('syndications', {})
    logger.info(f'Syndications config: {config}')

    uploaders = []
    uploader_args = {
        'provider_dealer_id': provider_dealer_id,
        'icc_formatted_inventory': icc_formatted_inventory,
    }

    for partner, uploader_class in SUPPORTED_UPLOADERS.items():
        partner_config = config.get(partner, {})
        if partner_config.get('active', False):
            uploaders.append(uploader_class(**uploader_args, config=partner_config))

    return uploaders
