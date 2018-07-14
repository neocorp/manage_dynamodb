import boto3


class ClientFactory:
    def __init__(self, client):
        self._client = boto3.client(client, region_name="eu-west-3")

    def get_client(self):
        return self._client


class DynamoDBClient(ClientFactory):
    def __init__(self):
        super().__init__('dynamodb')