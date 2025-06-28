import base64, os, random, boto3
from botocore.exceptions import NoCredentialsError, ClientError
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

access_key_id = os.getenv("aws_access_key_id")
secret_access_key = os.getenv("aws_secret_access_key")


boto3_config = Config(region_name='ap-south-1', signature_version='s3v4')

s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, config=boto3_config)


def upload_to_aws(local_file_path, s3_file_name, bucket_name, private):

    try:
        if local_file_path.endswith('.pdf'):
            contentType = 'application/pdf'

        elif local_file_path.endswith('.wav'):
            contentType = 'audio/wav'

        elif local_file_path.endswith('.mp4'):
            contentType = 'video/mp4'

        elif local_file_path.endswith('.webm'):
            contentType = 'video/webm'

        else:
            contentType = 'image/jpg'

        extra_args = {'ContentType': contentType, 'ContentDisposition': 'inline'}

        s3.upload_file(local_file_path, bucket_name, s3_file_name, ExtraArgs=extra_args)

        
        if not private:
            s3.put_object_acl(Bucket=bucket_name, Key=s3_file_name, ACL='public-read')


        if local_file_path != 'error_5418881.png' and local_file_path != 'error_type.png':
            os.remove(local_file_path)

        return f'https://{bucket_name}.s3.ap-south-1.amazonaws.com/{s3_file_name}'

    except Exception as e:
        print(e)
        #return 'error'
        return str(e)



def create_folder(bucket_name, directory_name):
    try:
        s3.put_object(Bucket=bucket_name, Key=(directory_name + '/'))
        return 'ok'
    except Exception as e:
        print(e)
        return 'error'

