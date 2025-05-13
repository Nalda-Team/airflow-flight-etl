from google.cloud import storage
import json
import os
def dump_in_json(json_data, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8-sig') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2, default=str)

def upload_json_to_gcp(bucket_name, source_file_path, destination_blob):
   storage_client = storage.Client()
   bucket = storage_client.bucket(bucket_name)
   blob = bucket.blob(destination_blob)
   blob.upload_from_filename(source_file_path)

def remove_uploaded_file(output_path):
   if os.path.exists(output_path):
       os.remove(output_path)