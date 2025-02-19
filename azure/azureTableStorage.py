from azure.data.tables import TableClient, TableServiceClient
from azure.core.exceptions import ResourceExistsError, HttpResponseError, ResourceNotFoundError

class Table(object):
    def __init__(self, config):
        if isinstance(config, dict):
            self.access_key = config.get('access_key', '')
            self.endpoint_suffix = config.get('endpoint_suffix', '')
            self.account_name = config.get('account_name', '')
        else:
            self.access_key = getattr(config, 'access_key', '')
            self.endpoint_suffix = getattr(config, 'endpoint_suffix', '')
            self.account_name = getattr(config, 'account_name', '')

        self.endpoint = f"{self.account_name}.table.{self.endpoint_suffix}"
        self.connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.account_name};AccountKey={self.access_key};EndpointSuffix={self.endpoint_suffix}"

    def authentication_by_connection_string(self):
        print("Instantiate a TableServiceClient using a connection string")
        # [START auth_from_connection_string]
        with TableServiceClient.from_connection_string(conn_str=self.connection_string) as table_service_client:
            properties = table_service_client.get_service_properties()
            print(f"{properties}")
        # [END auth_from_connection_string]

    def create_if_not_exists(self, table_name):
        # [START create_table_if_not_exists]
        with TableServiceClient.from_connection_string(self.connection_string) as table_service_client:
            table_client = table_service_client.create_table_if_not_exists(table_name=table_name)
            # return table_client
        # [END create_table_if_not_exists]

    def create_entity(self, entity, table_name):
        self.create_if_not_exists(table_name=table_name)
        with TableClient.from_connection_string(conn_str=self.connection_string, table_name=table_name) as table_client:
            # try:
            #     table_client.create_table_if_not_exists()
            # except HttpResponseError:
            #     print('Table already exists.')
            
            try:
                resp = table_client.create_entity(entity=entity)
                return resp
            except ResourceExistsError:
                print('Entity already exists')

    def delete_entity(self, entity, table_name):
        with TableClient.from_connection_string(conn_str=self.connection_string, table_name=table_name) as table_client:
            table_client.delete_entity(row_key=entity["RowKey"], partition_key=entity["PartitionKey"])

    def list_all_entities(self, table_name):
        with TableClient.from_connection_string(self.connection_string, table_name=table_name + "list") as table:
            try:
                entities = list(table.list_entities())
                return entities
            except HttpResponseError:
                print('entities not found.')
    
    def update_entities(self, entities, table_name):
        self.create_if_not_exists(table_name=table_name)
        with TableClient.from_connection_string(self.connection_string, table_name=table_name) as table_client:
            for entity in entities:
                try:
                    existing_entity = table_client.get_entity(partition_key=entity['PartitionKey'], row_key=entity['RowKey'])
                    existing_updated_at = existing_entity.get('updated_at', None)

                    if existing_updated_at and entity.get('updated_at'):
                        if entity['updated_at'] > existing_updated_at:
                            table_client.update_entity(entity=entity, mode='merge')
                    else:
                        table_client.update_entity(entity=entity, mode='merge')

                except ResourceNotFoundError:
                    table_client.create_entity(entity=entity)
                
                except Exception as e:
                    print(f"Error occurred while processing entity {entity['RowKey']}: {e}")
                    raise