from google.cloud import storage
def create_placeholder_dir(bucket_name, dir_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    # 슬래시로 끝나도록
    blob = bucket.blob(dir_path.rstrip('/') + '/')
    # 빈 스트링 업로드
    blob.upload_from_string('')