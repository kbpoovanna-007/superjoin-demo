import os
import time
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import pyodbc
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Google Sheets setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = 'your_spreadsheet_id_here'
RANGE_NAME = 'Sheet1!A1:Z1000'  # Adjust as needed

# SQL Server setup
SERVER = 'your_server_name'
DATABASE = 'your_database_name'
USERNAME = 'your_username'
PASSWORD = 'your_password'

def get_google_sheets_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('sheets', 'v4', credentials=creds)

def get_sql_connection():
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
    return pyodbc.connect(conn_str)

def read_sheet(service):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    return result.get('values', [])

def write_to_sheet(service, values):
    body = {'values': values}
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
        valueInputOption='USER_ENTERED', body=body).execute()

def read_from_sql():
    conn = get_sql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM your_table_name")
    rows = cursor.fetchall()
    conn.close()
    return [list(row) for row in rows]

def write_to_sql(data):
    conn = get_sql_connection()
    cursor = conn.cursor()
    for row in data:
        cursor.execute("INSERT INTO your_table_name VALUES (?, ?, ?)", row)  # Adjust SQL and parameters as needed
    conn.commit()
    conn.close()

class SheetHandler(FileSystemEventHandler):
    def __init__(self, service):
        self.service = service

    def on_modified(self, event):
        if event.src_path.endswith('token.json'):
            print("Google Sheet modified, updating SQL...")
            sheet_data = read_sheet(self.service)
            write_to_sql(sheet_data)

class SQLHandler(FileSystemEventHandler):
    def __init__(self, service):
        self.service = service

    def on_modified(self, event):
        if event.src_path.endswith('.mdf'):  # Adjust file extension as needed
            print("SQL database modified, updating Google Sheet...")
            sql_data = read_from_sql()
            write_to_sheet(self.service, sql_data)

def main():
    service = get_google_sheets_service()
    
    sheet_observer = Observer()
    sheet_handler = SheetHandler(service)
    sheet_observer.schedule(sheet_handler, path='.', recursive=False)
    sheet_observer.start()

    sql_observer = Observer()
    sql_handler = SQLHandler(service)
    sql_observer.schedule(sql_handler, path='.', recursive=False)
    sql_observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        sheet_observer.stop()
        sql_observer.stop()
    sheet_observer.join()
    sql_observer.join()

if __name__ == '__main__':
    main()