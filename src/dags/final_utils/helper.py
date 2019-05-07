from final_utils.atomic_plus import atomic_write_b

import boto3

def download_s3_files(bucket_nm='cscie29-data', s3_path = 'pset4/data/', copy_to='./'):
    """
    Vesrion 1.0.0 - Support downloading files from one s3 path,
                    by utilizing the enhanced atomciwriters which implements Suffix and binary writes
    """
    s3 = boto3.resource('s3'
                        #TO-DO: need to move the keys out, but .env doesn't work for this, also tried aws configure in command line.
                        ,aws_access_key_id='AKIASW6YFSLN2AFQ37PN'
                        ,aws_secret_access_key='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'
                    )
    bucket = s3.Bucket(bucket_nm)
    for obj in bucket.objects.all():
        key = obj.key
        if s3_path in key:
            body = obj.get()['Body'].read()
            print(key, type(body))
            with atomic_write_b(copy_to + key[len(s3_path):], 'w') as f:
                f.write(body)
    print('Files are successfully downloaded!')