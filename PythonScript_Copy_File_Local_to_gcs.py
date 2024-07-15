from google.cloud import storage
import os,glob

#defining variables:
src_bae_dir = os.environ.get('SRC_BASE_DIR')

#initiate client for gcs
gcs_client = storage.Client()

#fetch bucket name:
bucket = gcs_client.get_bucket('abhiretail')

#define a function to fetch data files from items list:
def get_file_path(src_base_dir):
    items = glob.glob(f'{src_bae_dir}/**', recursive=True)  #fetch files list from local
    return list(filter(lambda item : os.path.isfile(item) and item.endswith('part-00000'), items))

#Upload files to gcs from local:
files = get_file_path(src_bae_dir)
for file in files:
    print(f'Uploading {file} to gcs')
    blob_name = f'{file}'
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file)
