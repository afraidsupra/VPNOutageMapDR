import asyncio, boto3, os, logging
from dotenv import dotenv_values

secrets = dotenv_values(".env")

SourceDirectory = secrets["SOURCEDIR"]
OutageFileName = secrets["FILENAME"]
S3BucketName = secrets["S3BUCKETNAME"]
AWSAccessKey = secrets["ACCESSKEY"]
AWSSecretKey = secrets["SECRETKEY"]
RefreshTime = int(secrets["REFRESHTIME"])
UploadFileName = secrets["UPLOADNAME"]

SourceFilePath = SourceDirectory + OutageFileName

logging.basicConfig(
    filename='refreshlog.txt',
    filemode='a',
    format='[%(asctime)s] %(message)s',
    level=logging.INFO
)


async def main():
    while True:
        FileUpdated = await validateSource()
        if FileUpdated == True:
            UploadSuccess = await refreshJSON()
        logging.info(f'[SUCCESS] File uploaded successfully: {UploadSuccess}')
        await asyncio.sleep(RefreshTime)


async def validateSource(): # Function to determine that the source file has been generated
    if os.path.exists(SourceFilePath):
        logging.info(f'[SUCCESS] File {OutageFileName} successfully found in {SourceDirectory}.')
        return True
    else:
        logging.info(f'[ERROR] Unable to find file {OutageFileName} in {SourceDirectory}.')
        return False


async def refreshJSON():
    logging.info(f'[INFO] Attempting to update outages to {S3BucketName}')
    s3_client = boto3.client('s3',
                             aws_access_key_id=AWSAccessKey,
                             aws_secret_access_key=AWSSecretKey)
    try:
        response = s3_client.upload_file(SourceFilePath, S3BucketName, OutageFileName)
    except Exception as e:
        logging.info(f'[ERROR] {e}')
        return False
    return True


if __name__ == '__main__':
    logging.info(f'Enabling Data Refresh DR \n\nSource Location: {SourceDirectory}\nSource File Name: {OutageFileName}\nDestination S3: {S3BucketName}\nRefresh Time: {RefreshTime} seconds\nUploaded File Name: {OutageFileName}\n\nTo change these settings please modify the .env file.')
    asyncio.run(main())
