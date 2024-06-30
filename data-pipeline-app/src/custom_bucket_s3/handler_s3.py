from typing import Any
import logging
import boto3
from botocore.exceptions import NoCredentialsError

class HandleS3Bucket():
    def __init__(self) -> None:
        self.s3_client:Any = None
        self.bucket_name:str = None
        self.prefix:str = None
        self.remote_file_name:str = None
        self.full_remote_file_name:str = None

    def start_s3_client(self, access_key, secret_key, token_key):
        logging.info("Starting S3 Client...")
        try:
            session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=token_key
            )
            self.s3_client = session.client('s3')
            logging.info("S3 Client session opened!")
        except:
            logging.error("Something wrong while opening AWS Session/Client.")
            raise Exception("Something wrong while opening AWS Session/Client.")

    def set_s3_obj_attributes(self, bucket_name:str, remote_file_name:str, prefix:str|None=None):
        logging.info(f"Setting S3 attributes (bucket,remote_file,prefix): ({bucket_name}, {remote_file_name}, {prefix})")
        self.remote_file_name = remote_file_name
        self.prefix = prefix
        self.bucket_name = bucket_name

    def set_object_name_from_attributes(self):
        logging.info("Setting full_remote_file_name from prefix+remote_file_name")
        if self.prefix is not None:
            self.full_remote_file_name = f"{self.prefix}/{self.remote_file_name}"
        else:
            self.full_remote_file_name = f"{self.remote_file_name}"
        logging.info(f"full_remote_file_name = {self.full_remote_file_name}")

    def check_upload_or_delete_conditions_ok(self)->bool:
        remote_file_name_is_ok = (self.remote_file_name is not None)
        bucket_name_is_ok = (self.bucket_name is not None)
        return (remote_file_name_is_ok and bucket_name_is_ok)

    def upload_to_s3(self, local_file_name:str):
        print("Upload file to S3 Bucket:")
        try:
            if self.check_upload_or_delete_conditions_ok():
                logging.info(f"Upload/Delete attributes are ok!")
                self.set_object_name_from_attributes()
                logging.info(f"Uploading...")
                self.s3_client.upload_file(local_file_name, self.bucket_name, self.full_remote_file_name)
            else:
                logging.error(f"'local_file_name'={local_file_name} and/or 'bucket_name'={self.bucket_name} and/or 'remote_file_name'={self.remote_file_name} attributes not set.")
                raise ValueError(f"'local_file_name'={local_file_name} and/or 'bucket_name'={self.bucket_name} and/or 'remote_file_name'={self.remote_file_name} attributes not set.")
        except NoCredentialsError:
            logging.error("Invalid AWS Credentials")
            raise Exception("Invalid AWS Credentials")
        except ValueError as ve:
            raise Exception(ve)
        print("----------")

    def delete_from_s3(self):
        print("Delete file from S3 Bucket:")
        try:
            if self.check_upload_or_delete_conditions_ok():
                logging.info(f"Upload/Delete attributes are ok!")
                self.set_object_name_from_attributes()
                logging.info(f"Deleting...")
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=self.full_remote_file_name)
            else:
                logging.error(f"'remote_file_name'={self.remote_file_name} and/or 'bucket_name'={self.bucket_name} attributes not set.")
                raise ValueError(f"'remote_file_name'={self.remote_file_name} and/or 'bucket_name'={self.bucket_name} attributes not set.")
        except NoCredentialsError:
            logging.error("Invalid AWS Credentials")
            raise Exception("Invalid AWS Credentials")
        except ValueError as ve:
            raise Exception(ve)
        print("----------")

    def check_list_conditions_ok(self)->bool:
        bucket_is_ok = (self.bucket_name is not None)
        return (bucket_is_ok)

    def list_from_s3(self):
        print("List files from S3 Bucket:")
        try:
            if self.check_list_conditions_ok():
                logging.info(f"List action attributes are ok! Getting the List...")
                paginator = self.s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
                    for obj in page.get('Contents', []):
                        print(obj['Key'])
            else:
                logging.error(f"'bucket_name'={self.bucket_name} attribute not set.")
                raise ValueError(f"'bucket_name'={self.bucket_name} attribute not set.")
        except NoCredentialsError:
            logging.error("Invalid AWS Credentials")
            raise Exception("Invalid AWS Credentials")
        except ValueError as ve:
            raise Exception(ve)
        print("----------")