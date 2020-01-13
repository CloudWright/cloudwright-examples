import io
import csv
import os
import sqlalchemy
import re

s3 = CloudWright.get_module("aws_api").resource("s3")
email = CloudWright.get_module("gmail")
mysql = CloudWright.get_module("mysql")

# This script checks for new files in the s3://cloudwright-examples-file-automation/input directory.  On file receipt, it:
#  - converts the file from CSV to TSV
#  - moves the file to processed/
#  - emails the customer about the file receipt
#  - posts a message on slack that the file has arrived

bucket_name = 'cloudwright-examples-file-automation'
bucket = s3.Bucket(bucket_name)

files_processed = 0

# Scan for files in the input directory of the S3 bucket
for object_summary in bucket.objects.filter(Prefix='input'):

    if(object_summary.size > 0):
        detected_file = object_summary.key

        # Look for files in a subdirectory for each customer, corresponding to custmer ID
        match = re.match(r"input/([0-9]+)/([\w.]+)", detected_file)

        if match:
                customer_id = match.group(1)
                file_name = match.group(2)
                target_key = f'processed/{customer_id}/{file_name}'
                print(f"Detected a new file for customer {customer_id}: {file_name}")        
                print(f"Copying file to {target_key}")
                
                # Lightweight reformatting -- split the file on comma, and join it on tabs 
                output = list(map(lambda l: "\t".join(l.decode().split(",")), object_summary.get()['Body'].iter_lines()))
                new_file = s3.Object(bucket_name, target_key)

                # Write the reformatted file to the processed directory.
                new_file.put(Body="\n".join(output))

                # Look up the customer by ID in the file automation database
                customer = mysql.execute(sqlalchemy.text("SELECT email,name FROM customers where ID=:customer_id"), customer_id=customer_id).fetchone()
                customer_email = customer[0]
                customer_name = customer[1]
                print("Found owner for file: {customer_name}, contact: {customer_email}")

                # Use the linked email sender module to send the customer an email to let them know the file has arrived
                print("Notifying customer about file receipt")
                email.send_email("We got your file!", f"We received your file {file_name} and are processing it now.  Stay tuned!", customer_email)

                # Clean up the input file so we won't find it next run.
                print('Removing input file')
                s3.Object(bucket_name, detected_file).delete()
                files_processed += 1

CloudWright.response.set_value({'processed_files': files_processed})
