import logging
import boto3
from botocore.exceptions import ClientError

class S3Utils():

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.s3_client = boto3.client('s3')

    def upload_file(self,file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None :
            object_name = 'fotos/'+file_name
        # Upload the file
        try :
            response = self.s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e :
            logging.error(e)
            return False
        return True

    def retorne_TagObjeto(self):
        objects = self.s3.Bucket(name='gerbera').objects.all()
        for o in objects:
            response = self.s3_client.get_object_tagging(
                Bucket='gerbera',
                Key=o.key,
            )
            if len(response['TagSet']) > 0:
                print(response['TagSet'])

    def list_chaves_de_um_prefixo(self):
        #S3 list all keys with the prefix 'photos/'
        s3 = boto3.resource('s3')
        for bucket in s3.buckets.all():
            for obj in bucket.objects.filter(Prefix='fotos/'):
                print('{0}'.format(obj.key))

