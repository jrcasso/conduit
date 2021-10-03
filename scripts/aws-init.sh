set -x

awslocal s3 mb s3://ingress
awslocal s3 mb s3://egress
awslocal sqs create-queue --queue-name ingress-queue
awslocal sqs create-queue --queue-name egress-queue
awslocal sns create-topic --name ingress-topic
awslocal sns create-topic --name egress-topic
awslocal s3api put-bucket-notification --bucket ingress --notification-configuration 'QueueConfiguration={Event="s3:ObjectCreated:Put",Queue="ingress-queue"},TopicConfiguration={Event="s3:ObjectCreated:Put",Topic="arn:aws:sns:us-east-1:000000000000:ingress-topic"}'
awslocal s3api put-bucket-notification --bucket egress --notification-configuration 'QueueConfiguration={Event="s3:ObjectCreated:Put",Queue="egress-queue"},TopicConfiguration={Event="s3:ObjectCreated:Put",Topic="arn:aws:sns:us-east-1:000000000000:egress-topic"}'

set +x
