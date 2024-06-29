import logging
from custom_bucket_s3.handler_s3 import HandleS3Bucket
from custom_bucket_s3.aws_credentials import AWSCredentials, BucketArgs

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    filemode='w',
    filename="mainlog.log", 
    encoding='utf-8', 
    level=logging.DEBUG
    )

FILE_NAME = 'raw-data-b3custom/DataB3_20240628.parquet'

c = AWSCredentials()
b = BucketArgs()
hs3 = HandleS3Bucket()
hs3.start_s3_client(c.ACCESS_KEY, c.SECRET_KEY, c.TOKEN_KEY)
# hs3.set_s3_obj_attributes(b.BUCKET_NAME, FILE_NAME, b.PREFIX)
# hs3.upload_to_s3()
# hs3.list_from_s3()
hs3.set_s3_obj_attributes(b.BUCKET_NAME, None, None, 'raw/DataB3_20240628.parquet')
hs3.delete_from_s3()