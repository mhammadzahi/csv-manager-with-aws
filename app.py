import pandas as pd
import os
import random
import string
import requests
from urllib.parse import urlparse
from s3_uploader import upload_to_aws

# Load the original CSV
df = pd.read_csv("025-6-19_1715.csv")

# Output directory for images
output_dir = "downloaded_images"
os.makedirs(output_dir, exist_ok=True)

# Helpers
def sanitize_filename(value):
    return "".join(c if c.isalnum() else "_" for c in str(value))

def download_image(url, filename):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(os.path.join(output_dir, filename), "wb") as f:
            f.write(response.content)
        print(f"✅ Downloaded: {filename}")

    except Exception as e:
        print(f"❌ Failed: {url} | Reason: {e}")

# Process and update the 'images' column
updated_image_names = []

for _, row in df.iterrows():
    images = str(row['images']).split('|')
    property_ref_no = sanitize_filename(row.get('property_ref_no', 'ref'))
    sub_locality = sanitize_filename(row.get('sub_locality', 'sub'))
    tower_name = sanitize_filename(row.get('tower_name', 'tower'))

    new_filenames = []
    for img_url in images:
        img_url = img_url.strip()
        if img_url:
            rand = ''.join(random.choices(string.digits, k=15))
            ext = os.path.splitext(urlparse(img_url).path)[-1] or ".jpg"
            filename = f"{property_ref_no}_{sub_locality}_{tower_name}_{rand}{ext}"
            download_image(img_url, filename)
            aws_s3_file_name = upload_to_aws(os.path.join(output_dir, filename), f'5000/{filename}', 'propertyphotosabudhabi', False)
            #print(aws_s3_file_name)
            new_filenames.append(aws_s3_file_name)

    updated_image_names.append("**".join(new_filenames))
    print('------------------------')

df['images'] = updated_image_names

# Save new CSV
df.to_csv("bayut_with_renamed_images.csv", index=False)
print("✅ CSV updated and saved as 'bayut_with_renamed_images.csv'")





