## Environment variables
```
export AWS_REGION=
export AWS_ACCESS_KEY_ID=[1]
export AWS_SECRET_ACCESS_KEY=[1]
export AWS_S3_URL=[http://localhost:4572]
export BUCKET_NAME=[local-covid-bucket]
export API_KEYS=[a,b,c]
```

## Start development environment
`localstack start`
`aws --endpoint-url=http://localhost:4572 s3 mb s3://local-covid-bucket`
`serverless offline`