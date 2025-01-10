import boto3
import os

def data_ingestion(bucket_name, local_folder, file_or_files = 'all'):
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket=bucket_name)

    file_count = 0
    if 'Contents' in objects:
        if file_or_files == 'all':
            
            print('Downloading all the files from the bucket...')
            for obj in objects['Contents']:
                
                file_count +=1
                
                print(f"File Number '{file_count}' Info: {obj}")
                
                file_key =  obj['Key']
                local_file_path = os.path.join(local_folder, os.path.basename(file_key))
                
                s3_client.download_file(bucket_name, file_key, local_file_path)
            
            print(f"Total '{file_count}' files from the S3 bucket is/are downloaded in the path '{local_folder}'.")
        
        else:
            print('Downloading the provided file....')
            os.makedirs(local_folder, exist_ok=True)
            local_file_path = os.path.join(local_folder, os.path.basename(file_or_files))
            s3_client.download_file(bucket_name, file_or_files, local_file_path)
            print(f"File '{file_or_files}' downloaded from the s3 bucket.")
    
    else:
        print('No files in the bucket..')