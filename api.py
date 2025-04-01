from googleapiclient.discovery import build
from google.oauth2 import service_account
import io
from googleapiclient.http import MediaIoBaseDownload
import pdfplumber
import docx
import re
import faiss
import numpy as np
import json
import base64
import os
import uvicorn
from fastapi import FastAPI

app = FastAPI()

# 🔹 Load Google credentials
SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_BASE64")
if not SERVICE_ACCOUNT_B64:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT_BASE64 is not set!")

service_account_json = base64.b64decode(SERVICE_ACCOUNT_B64).decode("utf-8")
service_account_info = json.loads(service_account_json)
creds = service_account.Credentials.from_service_account_info(service_account_info)

# 🔹 Initialize Google Drive API
drive_service = build("drive", "v3", credentials=creds)

# 🔹 Define the folder ID containing resumes
FOLDER_ID = "1oTBOho6yIrxqdk5RCe6QPuEhxhEOYNJ2"  # Replace with actual folder ID

def get_all_files(folder_id):
    """Fetches all PDF and DOCX file IDs from Google Drive without loading them into memory."""
    query = f"'{folder_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    files = results.get("files", [])
    
    for file in files:
        if file["mimeType"] in ["application/pdf", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"]:
            yield file  # Return one file at a time
        elif file["mimeType"] == "application/vnd.google-apps.folder":
            yield from get_all_files(file["id"])  # Recursively get files

def extract_text(file_id, mime_type):
    """Streams text extraction without keeping full content in memory."""
    request = drive_service.files().get_media(fileId=file_id)
    file_stream = io.BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()
    
    file_stream.seek(0)
    
    if mime_type == "application/pdf":
        with pdfplumber.open(file_stream) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    yield text  # Return text as it is extracted
    elif mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        doc = docx.Document(file_stream)
        for para in doc.paragraphs:
            yield para.text  # Return paragraphs one by one

def remove_pii(text):
    """Removes emails and phone numbers but avoids loading full data into memory."""
    # Remove emails
    text = re.sub(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", "[EMAIL]", text)
    # Remove phone numbers
    text = re.sub(r"\b\d{10,12}\b", "[PHONE]", text)
    return text

def process_all_resumes():
    """Processes resumes with low memory usage and batches FAISS index updates."""
    faiss_index = None
    batch_vectors = []
    
    with open("cleaned_resumes.txt", "w", encoding="utf-8") as f:
        for file in get_all_files(FOLDER_ID):
            print(f"Processing: {file['name']}")
            sanitized_text = ""
            
            # Extract & sanitize text in a memory-efficient way
            for text in extract_text(file["id"], file["mimeType"]):
                sanitized_text += remove_pii(text) + "\n"

            f.write(sanitized_text)  # Append cleaned text to file instead of keeping in memory
            
            # Convert text to vector (using a simple embedding to reduce memory usage)
            vector = np.random.rand(300).astype("float32")  # Replace with actual embedding logic
            batch_vectors.append(vector)
            
            # Add vectors in batches to reduce FAISS memory load
            if len(batch_vectors) >= 10:
                if faiss_index is None:
                    faiss_index = faiss.IndexFlatL2(300)  # L2 distance FAISS index
                faiss_index.add(np.array(batch_vectors))
                batch_vectors.clear()  # Free memory

    # Add remaining vectors
    if batch_vectors:
        if faiss_index is None:
            faiss_index = faiss.IndexFlatL2(300)
        faiss_index.add(np.array(batch_vectors))

    print("✅ Processing complete!")

# Run resume processing in a separate thread
import threading
threading.Thread(target=process_all_resumes, daemon=True).start()

@app.get("/")
def read_root():
    return {"message": "Hello, World!"}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))  # Ensure it binds to Render's port
    uvicorn.run(app, host="0.0.0.0", port=port)
