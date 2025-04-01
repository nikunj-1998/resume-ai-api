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
from sklearn.feature_extraction.text import TfidfVectorizer

# 🔹 Load credentials from Render environment variable
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
    """Recursively fetches all PDF and DOCX files from Google Drive folder & subfolders."""
    query = f"'{folder_id}' in parents"
    results = drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    files = results.get("files", [])

    all_files = []
    for file in files:
        if file["mimeType"] in ["application/pdf", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"]:
            all_files.append(file)
        elif file["mimeType"] == "application/vnd.google-apps.folder":
            all_files.extend(get_all_files(file["id"]))  # Recursively get files from subfolders

    return all_files

def extract_text(file_id, mime_type):
    """Extracts text from a given Google Drive file."""
    request = drive_service.files().get_media(fileId=file_id)
    file_stream = io.BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request)
    
    done = False
    while not done:
        _, done = downloader.next_chunk()

    file_stream.seek(0)
    
    if mime_type == "application/pdf":
        with pdfplumber.open(file_stream) as pdf:
            return "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
    elif mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        doc = docx.Document(file_stream)
        return "\n".join([para.text for para in doc.paragraphs])
    
    return ""

def remove_pii(text):
    """Removes PII (names, emails, phone numbers, companies) using regex."""
    # Remove emails
    text = re.sub(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", "[EMAIL]", text)

    # Remove phone numbers (basic patterns)
    text = re.sub(r"\b\d{10,12}\b", "[PHONE]", text)

    # Remove company names (simple heuristic: words ending in 'Inc.', 'Corp.', 'Ltd.')
    text = re.sub(r"\b\w+\s+(Inc\.|Corp\.|Ltd\.)\b", "[COMPANY]", text)

    # Remove common first and last names (using a simple dictionary approach)
    common_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily"]  # Extend as needed
    for name in common_names:
        text = re.sub(rf"\b{name}\b", "[NAME]", text, flags=re.IGNORECASE)

    return text

# 🔹 Extract all text first
all_files = get_all_files(FOLDER_ID)
print(f"✅ Found {len(all_files)} resumes!")

full_text = "\n".join([extract_text(file["id"], file["mimeType"]) for file in all_files])
print("✅ Extracted all resume text!")

# 🔹 Sanitize all text at once
sanitized_text = remove_pii(full_text)
print("✅ Removed PII from text!")

# 🔹 Tokenize and load into FAISS using TF-IDF
vectorizer = TfidfVectorizer()
vectors = vectorizer.fit_transform([sanitized_text]).toarray()

faiss_index = faiss.IndexFlatL2(vectors.shape[1])  # L2 distance index
faiss_index.add(vectors.astype(np.float32))  
print("✅ Text indexed in FAISS using TF-IDF!")

# 🔹 Save cleaned text (optional)
with open("cleaned_resumes.txt", "w", encoding="utf-8") as f:
    f.write(sanitized_text)

print("✅ Process complete!")
