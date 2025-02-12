import boto3, os, logging, time, threading, requests, json
from discord_webhook import DiscordWebhook  # this will be replaced by a ServiceNow webhook
from datetime import datetime, timedelta
from threading import Event
from dotenv import dotenv_values

secrets = dotenv_values(".env")

SourceDirectory = secrets["SOURCEDIR"]
OutageFileName = secrets["FILENAME"]
S3BucketName = secrets["S3BUCKETNAME"]
AWSAccessKey = secrets["ACCESSKEY"]
AWSSecretKey = secrets["SECRETKEY"]
RefreshTimeSeconds = int(secrets["REFRESHTIME"])
UploadFileName = secrets["UPLOADNAME"]
S3address = secrets["S3ADDRESS"]
webhookURL = secrets["WEBHOOK"]

SourceFilePath = SourceDirectory + OutageFileName
JSONStatus = Event()
killThreads = Event()
activateAlert = Event()
alertFiring = Event()
manualJSONCopy = Event()

logging.basicConfig(
    filename='refreshlog.txt',
    filemode='a',
    format='[%(asctime)s] %(message)s',
    level=logging.INFO
)


def main():
    killThreads.clear()
    logging.info(f'Main thread started')
    inputThread = threading.Thread(target=takeInput, args=(JSONStatus, killThreads, activateAlert, manualJSONCopy, ))
    checkThread = threading.Thread(target=checkJSON, args=(JSONStatus, killThreads, activateAlert, ))
    alertThread = threading.Thread(target=alertStatus, args=(killThreads, activateAlert, alertFiring, ))
    manualcopyThread = threading.Thread(target=manualJSONRefresh, args=(killThreads, manualJSONCopy, ))

    inputThread.start()
    checkThread.start()
    alertThread.start()
    manualcopyThread.start()

    inputThread.join()
    checkThread.join()
    alertThread.join()
    manualcopyThread.join()
    print(f'Successfully killed threads')
    logging.info(f'[INFO] Successfully closed application')


def alertStatus(killThreads, activateAlert, alertFiring):
    logging.info(f'Alert thread started')
    while not killThreads.is_set():
        if activateAlert.is_set():
            if not alertFiring.is_set():
                logging.info(f'[ALERT] Notification fired')
                alertFiring.set()
                # Webhook to notify of incident here, this will be replaced by a SNOW ticket alert
                webhook = DiscordWebhook(url=webhookURL, content='<@341534364968026124> alert triggered - JSON stale. Please verify.')
                response = webhook.execute()
        if not activateAlert.is_set() and alertFiring.is_set():
            logging.info(f'[INFO] Alert has been cleared. Sending resolution notification.')
            alertFiring.clear()
            # Webhook to notify of resolution here, this will be replaced by SNOW ticket resolution
            webhook = DiscordWebhook(url=webhookURL, content='<@341534364968026124> stale alert cleared.')
            response = webhook.execute()
        time.sleep(5)
    return

def takeInput(JSONStatus, killThreads, activateAlert, manualJSONCopy):
    logging.info(f'Input thread started, please wait for console')
    time.sleep(3) # this is to allow time for all threads to start
    while not killThreads.is_set():
        print(f'\n\n== Outage Map JSON Monitor ==\nstatus: Check current state of JSON file\nstart: Start backup copy process\nstop: Stop backup copy process\nquit: Quit application\n')
        userInput = input('#')
        if userInput == 'quit':
            print(f'Quitting')
            killThreads.set()
            logging.info(f'[INFO] User commenced script shutdown')
            return
        elif userInput == 'start':
            if not manualJSONCopy.is_set():
                if activateAlert.is_set():
                    logging.info(f'[INFO] User commenced manual transfer')
                    print(f'Starting backup transfer')
                    manualJSONCopy.set()
                    time.sleep(1) # All of these sleep statements are just to give the CLI a nicer feel
                elif not activateAlert.is_set():
                    print(f'JSON does not appear to be stale. Use command "start!" to start backup copy process anyway')
                    time.sleep(1)
            else:
                logging.info(f'[ERROR] User attempted to start backup transfer. Backup transfer already in progress.')
                print(f'Backup transfer process is already active.')
                time.sleep(1)
        elif userInput == 'start!':
            if not manualJSONCopy.is_set():
                logging.info(f'[INFO] User commenced manual transfer')
                print(f'Starting backup transfer')
                manualJSONCopy.set()
                time.sleep(1)
            else:
                logging.info(f'[ERROR] User attempted to start backup transfer. Backup transfer already in progress.')
                print(f'Backup transfer process is already active.')
        elif userInput == 'stop':
            if manualJSONCopy.is_set():
                logging.info(f'[INFO] Stopping backup copy process')
                manualJSONCopy.clear()
                time.sleep(1)
            elif not manualJSONCopy.is_set():
                logging.info(f'[ERROR] User attempted to stop backup copy process while process not active.')
                print(f'Backup copier process is not currently active.')
                time.sleep(1)
        elif userInput == 'status':
            print(f'JSON status is {'Stale' if JSONStatus.is_set() else 'Fresh'}')
            time.sleep(1)
            


def checkJSON(JSONStatus, killThreads, activateAlert):
    logging.info(f'JSON Check Thread started')
    while not killThreads.is_set():
        APIData = requests.get(S3address)
        jSONData = json.loads(APIData.content)
        rawRefreshTime = jSONData["ROWSET"]["createDateTime"]
        processRefreshTime = rawRefreshTime.replace('T', ' ')
        JSONRefreshTime = datetime.strptime(processRefreshTime, '%Y-%m-%d %H:%M:%S')

        WriteTime = datetime.utcnow() + timedelta(hours = 11)
        UpdateLatency = WriteTime - JSONRefreshTime
        logging.info(f'[INFO] Current Latency: {UpdateLatency.seconds}')
        if UpdateLatency.seconds > 600:
            logging.info(f'[ALERT] JSON stale, alerting.')
            activateAlert.set()
            JSONStatus.set()
        if UpdateLatency.seconds < 600:
            activateAlert.clear()
            JSONStatus.clear()
        time.sleep(30) # I need to add a way to interrupt this sleep, otherwise it delays the shutdown
    return

def manualJSONRefresh(killThreads, manualJSONCopy):
    logging.info(f'JSON Copier Thread started')
    while not killThreads.is_set():
        while manualJSONCopy.is_set():
            logging.info(f'[INFO] Attempting to transfer GeoJSON')
            FileUpdated = validateSource()
            if FileUpdated == True:
                UploadSuccess = refreshJSON()
            logging.info(f'[INFO] File uploaded successfully: {UploadSuccess}')
            time.sleep(RefreshTimeSeconds)
        time.sleep(5)
    return


def validateSource():
    if os.path.exists(SourceFilePath):
        logging.info(f'[SUCCESS] File {OutageFileName} successfully found in {SourceDirectory}.')
        return True
    else:
        logging.info(f'[ERROR] Unable to find file {OutageFileName} in {SourceDirectory}.')
        return False


def refreshJSON():
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
    logging.info(f'[INFO] Starting Outage Map JSON monitor & DR \n\nSource Location: {SourceDirectory}\nSource File Name: {OutageFileName}\nDestination S3: {S3BucketName}\nRefresh Time: {RefreshTimeSeconds} seconds\nUploaded File Name: {OutageFileName}\n\nTo change these settings please modify the .env file.')
    main()
