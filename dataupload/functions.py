import fsspec
import boto3

# Option 1, using BOTO3 library. Will only work for S3, not azure (it is the one i use regularly and i'm most confident will work for at least S3)
def boto3_upload(aws_region: str, aws_access_key: str, aws_secret_key: str, aws_bucket_name: str, file_name: str = 'transformed_2_dltins.csv', csv_local_path: str = 'downloads/') -> None:
    """
    Upload a local csv file to a remote s3 bucket using BOTO3 library
    Args:
    - aws_region (str): AWS region of the bucket
    - aws_access_key (str): AWS account access key
    - aws_secret_key (str): AWS account secret key
    - aws_bucket_name (str): Target AWS S3 bucket name
    - file_name (str): local file name, also to be used in the bucket. includes '.csv'
    - csv_local_path (str): local path for the csv file
    Return:
    - None
    """

    try:
        s3_client = boto3.client(service_name='s3', region_name=aws_region, aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
        s3_client.upload_file(csv_local_path+file_name, aws_bucket_name, file_name)
    except Exception as e:
        print(f'An unexpected error occurred: {e}')

## ------------------------ ##

# Option 2, using fsspec to upload to either S3 or Azure Blob.
def fsspec_upload(client_kwargs: dict, target_path: str, file_name: str = 'transformed_2_dltins.csv', csv_local_path: str = 'downloads/', target: str = 's3') -> None:
    """
    Upload a local csv file to a remote s3 bucket or azure container using fsspec library
    Args:
    - client_kwargs (str): Composition of arguments to be passed for azure or s3
    - target_path (str): remote path to store the file. bucket name for S3 or container uri for azure
    - file_name (str): local file name, also to be used in the bucket. includes '.csv'
    - csv_local_path (str): local path for the csv file
    - target (str): 's3' or 'azure', defines to where the file will be uploaded
    Return:
    - None
    """
    fs = fsspec.filesystem(target, client_kwargs = client_kwargs)
    fs.cp(csv_local_path+file_name, f'{target_path}/{file_name}')
    # TODO add copy check