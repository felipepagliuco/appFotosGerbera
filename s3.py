import boto3

s3 = boto3.resource('s3')
my_bucket = s3.Bucket('gerbera')

# for bucket in s3.buckets.all():
#     print(bucket.name)


# for bucket in s3.buckets.all():
#     for obj in bucket.objects.filter(Prefix='fotos/Gazin/Anel/017557-120,00.JPG'):
#         print('{0}:{1}'.format(bucket.name, obj.key))
for obj in my_bucket.objects.filter(Prefix='fotos/Gazin/Anel/017557-120,00.JPG'):
    print('{0}:{1}'.format(my_bucket.name, obj.key))

# for my_bucket_object in my_bucket.objects.all():
#     print(my_bucket_object)