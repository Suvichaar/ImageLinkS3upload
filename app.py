import streamlit as st
import boto3
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests
import uuid
import re

# Load Google Sheets credentials from Streamlit secrets
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)
sheets_service = build('sheets', 'v4', credentials=credentials)

# Initialize AWS S3 client using Streamlit secrets
s3_client = boto3.client(
    's3',
    aws_access_key_id=st.secrets["AWS"]["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS"]["AWS_SECRET_ACCESS_KEY"],
    region_name=st.secrets["AWS"]["AWS_REGION"]
)

# Validate the S3 bucket name
def validate_bucket_name(bucket_name):
    if not re.match(r'^[a-z0-9][a-z0-9.-_]{1,61}[a-z0-9]$', bucket_name):
        st.error(f"Invalid bucket name: {bucket_name}. It must follow AWS S3 naming conventions.")
        return False
    return True

# Upload image to S3 and return the custom domain URL
def upload_to_s3(url, bucket_name):
    try:
        response = requests.get(url)
        response.raise_for_status()
        key = str(uuid.uuid4())
        s3_client.put_object(
            Bucket=bucket_name, 
            Key=key, 
            Body=response.content, 
            ContentType=response.headers['Content-Type']
        )
        return f'https://media.suvichaar.org/{key}'
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to download image from {url}: {e}")
        return None
    except Exception as e:
        st.error(f"Failed to upload image to S3: {e}")
        return None

# Convert column letter to index (A -> 0, B -> 1, etc.)
def column_letter_to_index(column_letter):
    column_letter = column_letter.upper()
    index = 0
    for char in column_letter:
        index = index * 26 + (ord(char) - ord('A') + 1)
    return index - 1  # Convert to zero-based index

# Process Google Sheet to upload images to S3
def process_sheet(spreadsheet_id, bucket_name, source_column='A', target_column='B'):
    try:
        source_index = column_letter_to_index(source_column)
        target_index = column_letter_to_index(target_column)

        sheet_data = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{source_column}:{target_column}"
        ).execute()

        rows = sheet_data.get('values', [])

        if not rows:
            st.warning("No data found in the specified range.")
            return

        for i, row in enumerate(rows):
            if len(row) > source_index and row[source_index]:  # Check if URL exists in source column
                url = row[source_index]
                s3_url = upload_to_s3(url, bucket_name)
                if s3_url:
                    if len(row) <= target_index:
                        row.extend([''] * (target_index + 1 - len(row)))  # Ensure enough columns
                    row[target_index] = s3_url
                    st.write(f"Uploaded {url} → {s3_url}")

        # Update Google Sheet with new S3 URLs
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{source_column}:{target_column}",
            valueInputOption='RAW',
            body={'values': rows}
        ).execute()

        st.success("Sheet updated successfully with S3 URLs.")

    except Exception as e:
        st.error(f"Error processing sheet: {e}")

# Streamlit UI
st.title("Image Uploader to S3")

spreadsheet_id = st.text_input("Google Spreadsheet ID")
bucket_name = st.text_input("S3 Bucket Name")
source_column = st.text_input("Source Column (default: A)", "A").strip().upper()
target_column = st.text_input("Target Column (default: B)", "B").strip().upper()

if st.button("Start Upload"):
    if spreadsheet_id and bucket_name:
        if validate_bucket_name(bucket_name):
            process_sheet(spreadsheet_id, bucket_name, source_column, target_column)
    else:
        st.error("Please provide both Spreadsheet ID and Bucket Name.")
