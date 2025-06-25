import os


class DBConnectionConfig:
    def __init__(self):
        self.host = None
        self.user = None
        self.port = None
        self.password = None
        self.database = None
        self.schema = None

    def validate(self):
        members = [attr for attr in dir(self) if not callable(
            getattr(self, attr)) and not attr.startswith("__")]
        missing_members = []
        for m in members:
            value = getattr(self, m)
            if value == "" or value is None:
                missing_members.append(m)
        if len(missing_members) > 0:
            error_text = 'DB Connection config is missing some members: ' + \
                         str(missing_members)
            raise Exception(error_text)

    def export_connection_dict(self):
        return {'host': self.host, 'port': self.port, 'user': self.user, 'password': self.password,
                'database': self.database, 'schema': self.schema}

    def get_schema(self):
        return self.schema


class ReplicaDBConnectionConfig(DBConnectionConfig):
    def __init__(self):
        super().__init__()
        self.host = os.getenv('DAS_RDS_HOST', None)
        self.port = os.getenv('DAS_RDS_PORT', 5432)
        self.user = os.getenv('ERP_REPLICAUSER', None)
        self.password = os.getenv('ERP_REPLICAPASSWORD', None)
        self.database = os.getenv(
            'ERP_REPLICA_DB', "erp_replica_sandbox")
        self.schema = os.getenv('ERP_REPLICA_SCHEMA', "airbyte")

        self.validate()


class DemandforecastDBConnectionConfig(DBConnectionConfig):
    def __init__(self):
        super().__init__()
        self.host = os.getenv('DAS_RDS_HOST', None)
        self.port = os.getenv('DAS_RDS_PORT', 5432)
        self.user = os.getenv('DAS_RDS_DEMANDFORECASTUSER', None)
        self.password = os.getenv('DAS_RDS_DEMANDFORECASTPASSWORD', None)
        self.database = os.getenv(
            'DEMANDFORECAST_DB', "erp_replica_sandbox")
        self.schema = os.getenv('DEMANDFORECAST_SCHEMA', "demandforecast_sandbox")
        self.validate()


class OrdertunerDBConnectionConfig(DBConnectionConfig):
    def __init__(self):
        super().__init__()
        self.host = os.getenv('DAS_RDS_HOST', None)
        self.port = os.getenv('DAS_RDS_PORT', 5432)
        self.user = os.getenv('DAS_RDS_ORDERTUNERUSER', None)
        self.password = os.getenv('DAS_RDS_ORDERTUNERPASSWORD', None)
        self.database = os.getenv('ORDERTUNER_DB', "erp_replica_sandbox")
        self.schema = os.getenv('ORDERTUNER_SCHEMA', "ordertuner_sandbox")

        self.validate()


class BazarApiConnectionConfig:
    def __init__(self):
        self.oauth_client_id = os.environ.get('AD_OAUTH_CLIENT_ID')
        self.oauth_client_secret = os.environ.get('AD_OAUTH_CLIENT_SECRET')
        self.oauth_token_endpoint = os.environ.get('AD_OAUTH_TOKEN_ENDPOINT')
        self.bazar_oauth_scope = os.environ.get('BAZAR_OAUTH_SCOPE')
        self.bazar_api_url = os.environ.get('BAZAR_API_URL')

    def export_connection_dict(self):
        return {attr: self.__getattribute__(attr) for attr in dir(self) if
                not callable(getattr(self, attr)) and not attr.startswith("__")}


class BazarQueuesConnectionConfig:
    def __init__(self):
        self.aws_user = os.environ.get('AWS_BAZAR_SQS_USER')
        self.aws_key_id = os.environ.get('AWS_BAZAR_SQS_KEYID')
        self.aws_key_secret = os.environ.get('AWS_BAZAR_SQS_KEYSECRET')
        self.aws_sqs_ordersuggestions = os.environ.get('AWS_BAZAR_SQS_ORDERSUGGESTIONS')
        self.aws_sqs_changesuggestions = os.environ.get('AWS_BAZAR_SQS_CHANGESUGGESTIONS')

    def export_connection_dict(self):
        return {attr: self.__getattribute__(attr) for attr in dir(self) if
                not callable(getattr(self, attr)) and not attr.startswith("__")}


class ClickhouseConnectionConfig(DBConnectionConfig):
     def __init__(self):
        super().__init__()
        self.host = os.getenv('DWH_HOST', None)
        self.port = os.getenv('DWH_PORT', 5432)
        self.user = os.getenv('DWH_USER', None)
        self.password = os.getenv('DWH_PASSWORD', None)
        self.database = os.getenv('DWH_DBNAME', "default")
        self.schema = "default"

        self.validate()


