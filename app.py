import streamlit as st
import boto3
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests
import uuid

# Load Google Sheets credentials from Streamlit secrets
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)
sheets_service = build('sheets', 'v4', credentials=credentials)

# Initialize AWS S3 client using Streamlit secrets
s3_client = boto3.client(
    's3',
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    region_name=st.secrets["AWS_REGION"]
)

def upload_to_s3(url, bucket_name):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
        key = str(uuid.uuid4())
        s3_client.put_object(
            Bucket=bucket_name, 
            Key=key, 
            Body=response.content, 
            ContentType=response.headers['Content-Type']
        )
        return f'https://{bucket_name}.s3.amazonaws.com/{key}'
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to download image from {url}: {e}")
        return None
    except Exception as e:
        st.error(f"Failed to upload image to S3: {e}")
        return None

def process_sheet(spreadsheet_id, bucket_name, source_column='A', target_column='B'):
    sheet_range = f"{source_column}:{target_column}"
    try:
        sheet = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=sheet_range
        ).execute()
        rows = sheet.get('values', [])

        source_index = ord(source_column.upper()) - ord('A')
        target_index = ord(target_column.upper()) - ord('A')

        for i, row in enumerate(rows):
            if len(row) > source_index and row[source_index]:  # Check if URL exists in source column
                url = row[source_index]
                s3_url = upload_to_s3(url, bucket_name)
                if s3_url:
                    if len(row) <= target_index:
                        row.extend([''] * (target_index + 1 - len(row)))  # Ensure row has enough columns
                    row[target_index] = s3_url
                    st.write(f"Uploaded {url} to {s3_url}")
        
        # Update Google Sheet with new URLs
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=sheet_range,
            valueInputOption='RAW',
            body={'values': rows}
        ).execute()
        st.success("Sheet updated successfully with S3 URLs.")
    
    except Exception as e:
        st.error(f"Error processing sheet: {e}")

# Streamlit Interface
st.title("Image Uploader")

# Input fields
spreadsheet_id = st.text_input("Spreadsheet ID")
bucket_name = st.text_input("Bucket Name")
source_column = st.text_input("Source Column (default A)", "A")
target_column = st.text_input("Target Column (default B)", "B")

# Button to trigger upload process
if st.button("Start Upload"):
    if spreadsheet_id and bucket_name:
        process_sheet(spreadsheet_id, bucket_name, source_column, target_column)
    else:
        st.error("Please provide both Spreadsheet ID and Bucket Name.")
