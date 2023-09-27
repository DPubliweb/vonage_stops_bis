# 1. Importer toutes les bibliothèques nécessaires
from flask import Flask, request
import logging
import os
import csv
import io
import boto3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import psycopg2
from dotenv import load_dotenv
from botocore.config import Config
from apscheduler.schedulers.background import BackgroundScheduler

# 2. Configurations générales
app = Flask(__name__)
scheduler = BackgroundScheduler()
load_dotenv()
# AWS S3 Configuration
s3_config = Config(retries = {'max_attempts': 10, 'mode': 'standard'}, max_pool_connections=50)
access_key = os.environ.get("AWS_ACCESS_KEY")
secret_key = os.environ.get("AWS_SECRET_KEY")
s3_client = boto3.client('s3', config=s3_config, region_name='us-west-2', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

# Redshift Configuration
host = os.environ.get('REDSHIFT_HOST')
port = os.environ.get('REDSHIFT_PORT')
dbname = os.environ.get('REDSHIFT_DBNAME')
user = os.environ.get('REDSHIFT_USER')
password = os.environ.get('REDSHIFT_PASSWORD')

#def csv_empty():
#    load_dotenv()
#    access_key = os.environ.get("AWS_ACCESS_KEY")
#    secret_key = os.environ.get("AWS_SECRET_KEY")
#    s3 = boto3.client('s3', region_name='us-west-2', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
#    s3.put_object(Bucket='data-vonage', Key='delivery-report.csv', Body='')
#    logging.debug("CSV emptied")

scope = ['https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive"]

# Google Sheets Configuration
TYPE = os.environ.get("TYPE")
PROJECT_ID = os.environ.get("PROJECT_ID")
PRIVATE_KEY_ID = os.environ.get("PRIVATE_KEY_ID")
PRIVATE_KEY = os.environ.get("PRIVATE_KEY").replace("\\n", "\n")
CLIENT_EMAIL = os.environ.get("CLIENT_EMAIL")
CLIENT_ID = os.environ.get("CLIENT_ID")
AUTH_URI = os.environ.get("AUTH_URI")
TOKEN_URI = os.environ.get("TOKEN_URI")
AUTH_PROVIDER_X509_CERT_URL = os.environ.get("AUTH_PROVIDER_X509_CERT_URL")
CLIENT_X509_CERT_URL = os.environ.get("CLIENT_X509_CERT_URL")

creds = ServiceAccountCredentials.from_json_keyfile_dict({
    "type": TYPE,
    "project_id": PROJECT_ID,
    "private_key_id": PRIVATE_KEY_ID,
    "private_key": PRIVATE_KEY,
    "client_email": CLIENT_EMAIL,
    "client_id": CLIENT_ID,
    "auth_uri": AUTH_URI,
    "token_uri": TOKEN_URI,
    "auth_provider_x509_cert_url": AUTH_PROVIDER_X509_CERT_URL,
    "client_x509_cert_url": CLIENT_X509_CERT_URL
}, scope)

client = gspread.authorize(creds)

def create_redshift_connection():
    return psycopg2.connect(
        host='pw-cluster.cq6jh9anojbf.us-west-2.redshift.amazonaws.com',
        port=5439,
        dbname=dbname,
        user=user,
        password=password
)

def append_to_sheet_1(data, lastname, firstname, origine):
    # Accédez à la feuille Google par son nom.
    sheet = client.open("Réponses - Publiweb").sheet1

    # Convertissez le dictionnaire en une liste pour le garder simple
    # Vous pouvez personnaliser cet ordre selon la structure de votre feuille.
    row = [data['msisdn'], data['text'], data['keyword'], data['message-timestamp']]
    
    # Ajoutez les données à la dernière ligne
    sheet.append_row(row)

def append_to_sheet_2(data):
    # Accédez à la feuille Google par son nom.
    sheet = client.open("Réponses - Publiweb").worksheet('Route 2')

    # Convertissez le dictionnaire en une liste pour le garder simple
    # Vous pouvez personnaliser cet ordre selon la structure de votre feuille.
    row = [data['msisdn'], data['text'], data['keyword'], data['message-timestamp']]
    
    # Ajoutez les données à la dernière ligne
    sheet.append_row(row)

def append_to_sheet_nely(data, lastname, firstname, utm, zipcode, type_chauffage, email):
    # Accédez à la feuille Google par son nom.
    sheet = client.open("Réponses - Nely").sheet1

    # Convertissez le dictionnaire en une liste pour le garder simple
    # Vous pouvez personnaliser cet ordre selon la structure de votre feuille.
    row = [data['msisdn'], data['text'], data['message-timestamp'],lastname, firstname, utm, zipcode, type_chauffage, email ]
    
    # Ajoutez les données à la dernière ligne
    sheet.append_row(row)

def append_to_sheet_publiweb(data, lastname, firstname, utm, zipcode, type_chauffage, email):
    # Accédez à la feuille Google par son nom.
    sheet = client.open("Audit - Publiweb").sheet1

    # Convertissez le dictionnaire en une liste pour le garder simple
    # Vous pouvez personnaliser cet ordre selon la structure de votre feuille.
    row = [data['msisdn'], data['text'], data['message-timestamp'],lastname, firstname, utm, zipcode, type_chauffage, email ]
    
    # Ajoutez les données à la dernière ligne
    sheet.append_row(row)


def phone_exists_in_sheet_1(phone_number):
    # Obtenez toutes les données de la première colonne (index 0)
    worksheet = client.open("Réponses - Publiweb").sheet1
    column_data = worksheet.col_values(1) # Si vous utilisez `gspread`
    return phone_number in column_data

def phone_exists_in_sheet_2(phone_number):
    # Obtenez toutes les données de la première colonne (index 0)
    worksheet = client.open("Réponses - Publiweb").worksheet('Route 2')
    column_data = worksheet.col_values(1) # Si vous utilisez `gspread`
    return phone_number in column_data

def phone_exists_in_sheet_nely(phone_number):
    # Obtenez toutes les données de la première colonne (index 0)
    worksheet = client.open("Réponses - Nely").sheet1
    column_data = worksheet.col_values(1) # Si vous utilisez `gspread`
    return phone_number in column_data

def phone_exists_in_sheet_pw(phone_number):
    # Obtenez toutes les données de la première colonne (index 0)
    worksheet = client.open("Audit - Publiweb").sheet1
    column_data = worksheet.col_values(1) # Si vous utilisez `gspread`
    return phone_number in column_data


def get_data_from_redshift(msisdn): #base nely reduite
    conn = create_redshift_connection()
    try:
        with conn.cursor() as cursor:
            query = "SELECT tel_global, lastname, firstname, utm, zipcode, type_chauffage, email FROM base_nelly_reduite WHERE tel_global = %s"
            cursor.execute(query, (msisdn,))
            results = cursor.fetchall()
            return results
    except Exception as e:
        # Log the error and/or handle it as needed
        logging.error(f"Error querying Redshift: {e}")
        return None
    finally:
        conn.close()

def get_data_from_redshift_2(msisdn): #base publiweb reduite
    conn = create_redshift_connection()
    try:
        with conn.cursor() as cursor:
            query = "SELECT tel_global, lastname, firstname, utm, zipcode, type_chauffage, email FROM vw_principale_tel_mobile WHERE tel_global = %s"
            cursor.execute(query, (msisdn,))
            results = cursor.fetchall()
            return results
    except Exception as e:
        # Log the error and/or handle it as needed
        logging.error(f"Error querying Redshift: {e}")
        return None
    finally:
        conn.close()

#clean_extract_leads_Nely.csv

def update_s3(data):
    try:
        existing_data = s3_client.get_object(Bucket='data-vonage', Key='stop-reports.csv')['Body'].read().decode('utf-8')
        new_data = [[data['msisdn'], data['keyword'], data['message-timestamp'][:10]]]
        csvfile = io.StringIO()
        writer = csv.writer(csvfile, delimiter=';')
        for line in csv.reader(existing_data.splitlines(), delimiter=';'):
            writer.writerow(line)
        writer.writerows(new_data)
        s3_client.put_object(Bucket='data-vonage', Key='stop-reports.csv', Body=csvfile.getvalue())
        logging.debug("Successfully wrote to S3")
    except Exception as e:
        logging.error(f"Error in update_s3: {str(e)}")
        return None

#start_time = datetime(2023, 5, 25, 10, 5, 0, tzinfo=timezone.utc)  
#scheduler.add_job(csv_empty, 'interval', weeks=1, next_run_time=start_time)
#scheduler.start()

import requests

@app.route('/webhooks/inbound-sms', methods=['GET', 'POST'])
def inbound_sms():
    logging.debug("Received a request at /webhooks/inbound-sms")
    logging.debug(f"Request content type: {request.content_type}")
    logging.debug(f"Request body: {request.get_data(as_text=True)}")

    data = {}
    # Extraction des données de la demande
    if request.is_json:
        logging.debug("Request is JSON")
        data = request.get_json()
    elif request.form:
        logging.debug("Request is form-data")
        data = {
            'msisdn': request.form.get('msisdn'),
            'text': request.form.get('text'),
            'keyword': request.form.get('keyword'),
            'message-timestamp': request.form.get('message-timestamp'),
            'api-key': request.form.get('api-key')
        }
    else:
        logging.warning("Request is neither JSON nor form-data")
        return "Requête invalide", 400
    
    # Ajout des données à la feuille principale et mise à jour de S3
    #append_to_sheet(data)
    results = get_data_from_redshift(data['msisdn'])
    if results:
        tel_global, lastname, firstname, utm, zipcode, type_chauffage, email = results[0]
        origine = "Nely"
        append_to_sheet_1(data, lastname, firstname, origine)
        print("Data from Nely")
        
        if tel_global and '1' == data['text']:
            if not phone_exists_in_sheet_nely(tel_global):
                append_to_sheet_nely(data, lastname, firstname, utm, zipcode, type_chauffage, email)
                url_publiweb = 'https://automation-vt-f29dcdcf11fd.herokuapp.com/lead_pblw/aIR7DvmX9cgTO55g8di6jvLPAvGBccm'
                headers_publiweb = {'Content-Type': 'application/json'}
                data_publiweb = {
                    'date': data['message-timestamp'],  # Utilisez une date dynamique si nécessaire
                    'telephone': tel_global,
                    'firstname': firstname,
                    'lastname': lastname,
                    'utm': utm,
                    'zip_code': zipcode,
                    'mode_chauffage': type_chauffage,
                    'email': email
                }
                print(data_publiweb, 'Nely')
                response = requests.post(url_publiweb, headers=headers_publiweb, json=data_publiweb)
                if response.status_code != 200:
                    logging.warning(f"Failed to send data to publiweb endpoint. Status code: {response.status_code}")
                else: 
                    print('Data sent to Nely CRM successfully')
            else:
                logging.info(f"Phone number {tel_global} already exists in the sheet, skipping entry and POST request.")
        print('Data got from Nely')
    else:
        results = get_data_from_redshift_2(data['msisdn'])
        if results:
            origine = "Publiweb"
            append_to_sheet_1(data, lastname, firstname, origine)

    return "Done SR !"
       


if __name__ == "__main__":
    app.run(host='0.0.0.0',port=8080,debug=True)
