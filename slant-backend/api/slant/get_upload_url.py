# app.py (Flask backend)

import boto3
from flask import Flask, request, jsonify
import os
from constants.keys import AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET_NAME
from utils.utils import log

def get_upload_url(filename):
    log(f'getting upload url for {filename}')

    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    try:
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': AWS_S3_BUCKET_NAME,
                'Key': filename,
                'ContentType': 'image/png'
            },
            ExpiresIn=60  # URL expires in 60 seconds
        )

        file_url = f"https://{AWS_S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{filename}"
        log(f'file url: {file_url}')

        return jsonify({
            'uploadUrl': presigned_url,
            'fileUrl': file_url
        })

    except Exception as e:
        print(f"Error generating presigned URL: {e}")
        return jsonify({'error': 'Could not generate URL'}), 500

