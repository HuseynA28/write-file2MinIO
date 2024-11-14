class MinIOClient:
    def __init__(self, server: str, access_key: str, secret_key: str, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            endpoint_url=server,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                print(f"Bucket '{self.bucket_name}' created successfully")
            else:
                print(f"Error checking bucket: {e}")
                raise

    async def save_telemetry(self, telemetry: Dict, table_name: str) -> bool:
        try:
            json_data = json.dumps(telemetry).encode('utf-8')
            filename = f"{table_name}.json"

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=filename,
                Body=json_data
            )
            return True

        except Exception as e:
            print(f"Error saving to MinIO: {str(e)}")
            return False