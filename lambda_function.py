import os
import json
import urllib.parse
import boto3

s3 = boto3.resource('s3')
sdb = boto3.client('sdb')

def seq_check(old_seq, new_seq, allow_key_mismatch):
    old_seq, old_key = old_seq.split(' ',1)
    new_seq, new_key = new_seq.split(' ',1)
    if old_key != new_key:
        return allow_key_mismatch
    lendif = len(new_seq)-len(old_seq)
    if lendif > 0:
        old_seq = ('0'*lendif)+old_seq
    elif lendif < 0:
        new_seq = ('0'*-lendif)+new_seq
    return old_seq <= new_seq

def delete_record(item, seq, attempts=30, **kwargs):
    for i in range(0, attempts):
        old_seq = sdb.get_attributes(
            DomainName=os.environ['SDB_DOMAIN'],
            ItemName=item,
            AttributeNames=['S3_Sequencer'],
            ConsistentRead=True
            )
        old_seq = dict(map(lambda x: (x['Name'],x['Value']), old_seq.get('Attributes',[]))).get('S3_Sequencer', None)
        if old_seq is None:
            print('Delete: old_seq is None: nothing to do.')
            return True # Nothing to do
        else:
            if not seq_check(old_seq, seq, False):
                print('Delete: We\'re obsolete.')
                return False # We're obsolete.
            try:
                sdb.delete_attributes(
                    DomainName=os.environ['SDB_DOMAIN'],
                    ItemName=item,
                    Expected={ 'Name': 'S3_Sequencer', 'Value': old_seq },
                    **kwargs
                    )
            except Exception as e:
                print('delete_record: on retry #{}, caught {}'.format(i,e))
            else:
                return True

def update_record(seq, *, ItemName, Attributes, old_key_cb=lambda x: None, attempts=30):
    for i in range(0, attempts):
        old = sdb.get_attributes(
            DomainName=os.environ['SDB_DOMAIN'],
            ItemName=ItemName,
            AttributeNames=['S3_Sequencer','S3_Key'],
            ConsistentRead=True,
            )
        old = dict(map(lambda x: (x['Name'],x['Value']), old.get('Attributes',[])))
        old_seq = old.get('S3_Sequencer', None)
        old_key = old.get('S3_Key', None)
        Expected = { 'Name': 'S3_Sequencer' }
        if old_seq is None:
            Expected['Exists'] = False
        else:
            if not seq_check(old_seq, seq, True):
                return False # We're obsolete.
            else:
                Expected['Value'] = old_seq
                if old_key != seq.split(' ',1)[1]:
                    old_key_cb(old_key) # An opportunity to delete the older/moved version of this file.
        try:
            sdb.put_attributes(
                DomainName=os.environ['SDB_DOMAIN'],
                ItemName=ItemName,
                Attributes=Attributes,
                Expected=Expected
            )
        except Exception as e:
            print('update_record: on retry #{}, caught {}'.format(i,e))
        else:
            return True

def handle_record(record):
    bucket = record['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
    seq = '{} {}'.format(record['s3']['object']['sequencer'], key)
    print((bucket,key))
    if record['eventName'].startswith('ObjectCreated:'):
        obj = s3.Object(bucket, key)
        sdb_attrs = dict(obj.metadata)
        sdb_attrs['S3_ContentType'] = obj.content_type
        sdb_attrs['S3_Key'] = key
        sdb_attrs['S3_Sequencer'] = seq
        Attributes = []
        for k,v in sdb_attrs.items():
            if k == 'tags':
                for tag in map(lambda x: x.strip(), sdb_attrs['tags'].split(',')):
                    Attributes.append({'Name': 'tags', 'Value': tag, 'Replace':True})
            else:
                Attributes.append({'Name': k, 'Value': v, 'Replace':True})
        if 'uuid' in sdb_attrs:
            print(sdb_attrs)
            update_record(
                seq=seq,
                ItemName=sdb_attrs['uuid'], 
                Attributes=Attributes,
                old_key_cb=lambda x: s3.Object(bucket, x).delete()
                )
            delete_record(sdb_attrs['uuid'], seq, Attributes=[{'Name':'tag','Value':''}])
    elif record['eventName'].startswith('ObjectRemoved:'):
        old_recs = sdb.select(SelectExpression='select itemName() from {domain} where S3_Key = "{key}"'.format(domain=os.environ['SDB_DOMAIN'], key=key.replace('\\','\\\\').replace('"','\\"')), ConsistentRead=True).get('Items',[])
        for old_rec in old_recs:
            delete_record(old_rec['Name'], seq)
    return
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

def lambda_handler(event, context):
    print(event)
    for record in event['Records']:
        handle_record(record)