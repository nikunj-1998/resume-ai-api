from googleapiclient.discovery import build
from google.oauth2 import service_account
import io
from googleapiclient.http import MediaIoBaseDownload
import pdfplumber
import docx
import spacy
import re
import faiss
import numpy as np
import json
import base64
import os

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

# 🔹 Load spaCy model for PII removal
nlp = spacy.load("en_core_web_sm")

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
    """Removes PII (names, emails, phone numbers, companies) from extracted text."""
    # Remove emails
    text = re.sub(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", "[EMAIL]", text)

    # Remove phone numbers (basic patterns)
    text = re.sub(r"\b\d{10,12}\b", "[PHONE]", text)

    # Remove names using spaCy
    doc = nlp(text)
    cleaned_text = []
    for token in doc:
        if token.ent_type_ == "PERSON":
            cleaned_text.append("[NAME]")
        elif token.ent_type_ in ["ORG", "COMPANY"]:
            cleaned_text.append("[COMPANY]")
        else:
            cleaned_text.append(token.text)
    
    return " ".join(cleaned_text)

# 🔹 Extract all text first
all_files = get_all_files(FOLDER_ID)
print(f"✅ Found {len(all_files)} resumes!")

full_text = "\n".join([extract_text(file["id"], file["mimeType"]) for file in all_files])
print("✅ Extracted all resume text!")

# 🔹 Sanitize all text at once
sanitized_text = remove_pii(full_text)
print("✅ Removed PII from text!")

# 🔹 Tokenize and load into FAISS
nlp_vectors = nlp(sanitized_text).vector
faiss_index = faiss.IndexFlatL2(nlp_vectors.shape[0])  # L2 distance index
faiss_index.add(np.array([nlp_vectors]))  
print("✅ Text indexed in FAISS!")

# 🔹 Save cleaned text (optional)
with open("cleaned_resumes.txt", "w", encoding="utf-8") as f:
    f.write(sanitized_text)

print("✅ Process complete!")
