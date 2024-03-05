import boto3
import time
import fitz  # PyMuPDF

# Initialize the AWS clients
textract = boto3.client('textract')
comprehend = boto3.client('comprehend')
s3 = boto3.client('s3')

def start_text_detection(bucket, document):
    """
    Starts the asynchronous text detection.
    """
    response = textract.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket,
                'Name': document
            }
        }
    )
    return response['JobId']

def is_job_complete(job_id):
    """
    Checks the status of the text detection job.
    """
    response = textract.get_document_text_detection(JobId=job_id)
    status = response['JobStatus']
    return status == 'SUCCEEDED'

def get_job_results(job_id):
    """
    Retrieves the results of a completed text detection job.
    """
    pages = []
    next_token = None
    while True:
        response = textract.get_document_text_detection(JobId=job_id, NextToken=next_token) if next_token else textract.get_document_text_detection(JobId=job_id)

        pages.extend(response['Blocks'])
        next_token = response.get('NextToken', None)
        if next_token is None:
            break
    
    return pages

def extract_text_positions(bucket, document):
    job_id = start_text_detection(bucket, document)
    print("Text detection job started with ID:", job_id)
    
    # Wait for the text detection job to complete
    while not is_job_complete(job_id):
        print("Waiting for text detection job to complete...")
        time.sleep(5)
    
    results = get_job_results(job_id)
    text_data = []
    for item in results:
        if item['BlockType'] == 'LINE':
            text_data.append({
                'Text': item['Text'],
                'Geometry': item['Geometry']['BoundingBox'],
                'Page': item['Page']  # Capture the page number if available
            })
    return text_data


def detect_pii(text):
    """
    Detects PII information in the provided text using Amazon Comprehend.
    """
    response = comprehend.detect_pii_entities(
        Text=text,
        LanguageCode='en'
    )
    return response['Entities']


def apply_redactions(pdf_path, text_data, pii_entities):
    # Open the PDF
    doc = fitz.open(pdf_path)

    for entity in pii_entities:
        # Extract the PII text using offsets
        pii_text = full_text[entity['BeginOffset']:entity['EndOffset']]
        
        for text_item in text_data:
            if pii_text in text_item['Text']:
                # Match the text item to its page
                page = doc[text_item['Page'] - 1]  # Adjust for zero-based index in PyMuPDF
                
                # Calculate redaction rectangle
                x0 = text_item['Geometry']['Left'] * page.rect.width
                y0 = text_item['Geometry']['Top'] * page.rect.height
                x1 = (text_item['Geometry']['Left'] + text_item['Geometry']['Width']) * page.rect.width
                y1 = (text_item['Geometry']['Top'] + text_item['Geometry']['Height']) * page.rect.height
                redact_rect = fitz.Rect(x0, y0, x1, y1)

                # Add redaction annotation
                page.add_redact_annot(redact_rect)

        # Apply all redactions on each page
        for page in doc:
            page.apply_redactions()

    redacted_pdf_path = 'redacted_' + pdf_path.split('/')[-1]
    doc.save(redacted_pdf_path)
    doc.close()
    return redacted_pdf_path


# Set your bucket name, subfolder, and document filename
bucket_name = 'docexamplebucketpii'
subfolder = 'test'
document_filename = 'eStatement_Feb2024.pdf'
document_path = f"{subfolder}/{document_filename}"

# Download the PDF from S3 to be processed locally
s3.download_file(bucket_name, document_path, document_filename)

# Extract text and positions
text_data = extract_text_positions(bucket_name, document_path)

# Flatten extracted text for PII detection
full_text = ' '.join([item['Text'] for item in text_data])
print(full_text)
# Detect PII
pii_entities = detect_pii(full_text)
print (pii_entities)
# Apply redactions to the PDF
redacted_pdf_path = apply_redactions(document_filename, text_data, pii_entities)
print(f"Redacted PDF saved to {redacted_pdf_path}")
