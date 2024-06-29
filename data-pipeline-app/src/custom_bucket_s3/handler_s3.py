from typing import Any
import boto3
from botocore.exceptions import NoCredentialsError

class HandleS3Bucket():
    def __init__(self) -> None:
        self.s3_client:Any = None
        self.bucket_name:str = None
        self.key_object_name:str = None
        self.file_name:str = None

    def start_s3_client(self, access_key, secret_key, token_key):
        try:
            session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=token_key
            )
            self.s3_client = session.client('s3')
        except:
            raise Exception("Something wrong while opening AWS Session/Client.")

    def set_s3_obj_attributes(self, bucket_name:str, file_name:str, prefix:str=None, object_name:str=None):
        self.file_name = file_name
        self.prefix = prefix
        self.bucket_name = bucket_name
        self.key_object_name = object_name

    def set_object_name_from_attributes(self):
        if self.key_object_name is None:
            self.key_object_name = self.file_name.split('/')[-1]
        if self.prefix is not None:
            self.key_object_name = f"{self.prefix}/{self.key_object_name}"

    def check_upload_conditions_ok(self)->bool:
        file_name_is_ok = (self.file_name is not None)
        bucket_name_is_ok = (self.bucket_name is not None)
        return (file_name_is_ok and bucket_name_is_ok)

    def upload_to_s3(self):
        try:
            if self.check_upload_conditions_ok():
                self.set_object_name_from_attributes()
                self.s3_client.upload_file(self.file_name, self.bucket_name, self.key_object_name)
            else:
                raise ValueError(f"'file_name'={self.file_name} and/or 'bucket_name'={self.bucket_name} attributes not set.")
        except NoCredentialsError:
            raise Exception("Invalid Credentials")
        except ValueError as ve:
            raise Exception(ve)

    def check_delete_conditions_ok(self)->bool:
        object_name_is_ok = (self.key_object_name is not None)
        prefix_is_ok = (self.prefix is None)
        bucket_is_ok = (self.bucket_name is not None)
        print(object_name_is_ok)
        print(prefix_is_ok)
        print(bucket_is_ok)
        print((bucket_is_ok and object_name_is_ok and prefix_is_ok))
        return (bucket_is_ok and object_name_is_ok and prefix_is_ok)

    def delete_from_s3(self):
        try:
            if self.check_delete_conditions_ok():
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=self.key_object_name)
            else:
                raise ValueError(f"'object_name'={self.file_name} and 'bucket_name'={self.bucket_name} attributes not set and/or 'prefix'={self.prefix} is not None.")
        except NoCredentialsError:
            raise Exception("Invalid Credentials")
        except ValueError as ve:
            raise Exception(ve)

    def check_list_conditions_ok(self)->bool:
        bucket_is_ok = (self.bucket_name is not None)
        return (bucket_is_ok)

    def list_from_s3(self):
        try:
            if self.check_list_conditions_ok():
                paginator = self.s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix):
                    for obj in page.get('Contents', []):
                        print(obj['Key'])
            else:
                raise ValueError(f"'bucket_name'={self.bucket_name} attribute not set.")
        except NoCredentialsError:
            raise Exception("Invalid Credentials")
        except ValueError as ve:
            raise Exception(ve)