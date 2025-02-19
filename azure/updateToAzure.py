from typing_extensions import TypedDict
from datetime import datetime
from azure import azureTableStorage
import logging
import os
from pandas import isna
from typing import Optional

# Existing HubspotTicket definition
class HubspotTicket(TypedDict, total=False):
    PartitionKey: str
    RowKey: str
    hs_ticket_id : int
    hubspot_owner_id : int
    closed_date : datetime
    createdate : datetime
    hs_lastmodifieddate : datetime
    content : str
    creator_name : str
    customer_code___erp : str
    hs_pipeline : str
    hs_pipeline_stage : str
    hs_primary_company_name : str
    issue_type : str
    item_name___id : str
    product : str
    si_jira_issue_id : str
    subject : str
    ticket_priority : str
    ticket_type : str
    defect : str
    regulatory_reportable_incident : bool
    updated_at : datetime

# New HubspotDeal class
class HubspotDeal(TypedDict, total=False):
    PartitionKey: str
    RowKey: str
    hs_deal_id: int
    dealname: str
    dealstage: str
    pipeline: str
    amount: float
    closedate: datetime
    createdate: datetime
    hs_lastmodifieddate: datetime
    current_status: Optional[str]
    installation_a_beds: Optional[int]
    hs_v2_date_entered_176875376: Optional[datetime]
    hs_v2_date_entered_appointmentscheduled: Optional[datetime]
    hs_v2_date_exited_appointmentscheduled: Optional[datetime]
    hs_v2_date_exited_qualifiedtobuy: Optional[datetime]
    hs_v2_date_entered_qualifiedtobuy: Optional[datetime]
    hs_v2_date_entered_decisionmakerboughtin: Optional[datetime]
    date_entered__demo_in_progress_: Optional[datetime]
    hs_v2_date_entered_contractsent: Optional[datetime]

# New HubspotOwners class
class HubspotOwners(TypedDict, total=False):
    PartitionKey: str
    RowKey: str
    id: int
    email: str
    firstName: str
    lastName: str
    userId: int
    createdAt: str
    updatedAt: str
    archived: bool
    # teamName: str
    type: str

class Config:
    access_key = os.environ.get('ASSCESS_KEY')
    endpoint_suffix = "core.windows.net"
    account_name = "tstbi"

config_obj = Config()

def escape_special_characters(value):
    """Escapes special characters like single quotes in SQL strings."""
    if isinstance(value, str):
        return value.replace("'", "''")
    return value

def format_boolean(val):
    """Handles boolean formatting."""
    if val == 'NULL' or isna(val):
        return 'NULL'
    elif str(val).lower() == 'true':
        return 'TRUE'
    elif str(val).lower() == 'false':
        return 'FALSE'
    return 'NULL'

def update_hubspot_tickets(tickets):
    logging.info('Inserting tickets to Azure Storage')
    table = azureTableStorage.Table(config=config_obj)
    entities = []

    for ticket in tickets:
        properties = ticket.properties
        updated_at = ticket.updated_at

        val_dict = {
            key: None if isna(value) or len(str(value)) == 0 else value 
            for key, value in properties.items()
        }

        entity: HubspotTicket = {
            "PartitionKey": "tickets",
            "RowKey": val_dict.get('hs_object_id', None),
            "hs_ticket_id": val_dict.get('hs_ticket_id', None),
            "hubspot_owner_id": val_dict.get('hubspot_owner_id', None),
            "closed_date": val_dict.get('closed_date', None),
            "createdate": val_dict.get('createdate', None),
            "hs_lastmodifieddate": val_dict.get('hs_lastmodifieddate', None),
            "content": escape_special_characters(val_dict.get('content', '')) if val_dict.get('content') else '',
            "creator_name": escape_special_characters(val_dict.get('creator_name', '')) if val_dict.get('creator_name') else '',
            "customer_code___erp": escape_special_characters(val_dict.get('customer_code___erp', '')) if val_dict.get('customer_code___erp') else '',
            "hs_pipeline": escape_special_characters(val_dict.get('hs_pipeline', '')) if val_dict.get('hs_pipeline') else '',
            "hs_pipeline_stage": escape_special_characters(val_dict.get('hs_pipeline_stage', '')) if val_dict.get('hs_pipeline_stage') else '',
            "hs_primary_company_name": escape_special_characters(val_dict.get('hs_primary_company_name', '')) if val_dict.get('hs_primary_company_name') else '',
            "issue_type": escape_special_characters(val_dict.get('issue_type', '')) if val_dict.get('issue_type') else '',
            "item_name___id": escape_special_characters(val_dict.get('item_name___id', '')) if val_dict.get('item_name___id') else '',
            "product": escape_special_characters(val_dict.get('product', '')) if val_dict.get('product') else '',
            "si_jira_issue_id": escape_special_characters(val_dict.get('si_jira_issue_id', '')) if val_dict.get('si_jira_issue_id') else '',
            "subject": escape_special_characters(val_dict.get('subject', '')) if val_dict.get('subject') else '',
            "ticket_priority": escape_special_characters(val_dict.get('ticket_priority', '')) if val_dict.get('ticket_priority') else '',
            "ticket_type": escape_special_characters(val_dict.get('ticket_type', '')) if val_dict.get('ticket_type') else '',
            "defect": format_boolean(val_dict.get('defect', None)),
            "regulatory_reportable_incident": format_boolean(val_dict.get('regulatory_reportable_incident', None)),
            "updated_at": updated_at
        }

        entities.append(entity)

    if entities:
        table.update_entities(entities=entities, table_name='hubspotticket')
        logging.info(f'Inserted {len(entities)} tickets to Azure Storage')
    else:
        logging.warning('No valid tickets to update in Azure Storage')

# Add the function for updating deals
def update_hubspot_deals(deals):
    logging.info('Inserting deals to Azure Storage')
    table = azureTableStorage.Table(config=config_obj)
    entities = []

    for deal in deals:
        properties = deal.properties
        updated_at = deal.updated_at

        val_dict = {
            key: None if isna(value) or len(str(value)) == 0 else value 
            for key, value in properties.items()
        }

        # Now define the entity using partition_key and row_key
        entity: HubspotDeal = {
            "PartitionKey": "deals",
            "RowKey": val_dict.get('hs_object_id', None),
            "hs_deal_id": val_dict.get('hs_object_id', None),
            "pipeline": escape_special_characters(val_dict.get('pipeline', '')) if val_dict.get('pipeline') else '',
            "dealname": escape_special_characters(val_dict.get('dealname', '')) if val_dict.get('dealname') else '',
            "dealstage": escape_special_characters(val_dict.get('dealstage', '')) if val_dict.get('dealstage') else '',
            "dealtype": escape_special_characters(val_dict.get('dealtype', '')) if val_dict.get('dealtype') else '',
            "installation_a_beds": val_dict.get('installation_a_beds', None),
            "current_status": val_dict.get('current_status', None),
            "amount": val_dict.get('amount', None),
            "hubspot_owner_id": val_dict.get('hubspot_owner_id', None),
            "updated_at": updated_at,
            "createdate": val_dict.get('createdate', None),
            "closedate": val_dict.get('closeddate', None),
            "hs_lastmodifieddate": val_dict.get('hs_lastmodifieddate', None),
            "hs_v2_date_entered_176875376": val_dict.get('hs_v2_date_entered_176875376', None),
            "hs_v2_date_entered_appointmentscheduled": val_dict.get('hs_v2_date_entered_appointmentscheduled', None),
            "hs_v2_date_exited_appointmentscheduled": val_dict.get('hs_v2_date_exited_appointmentscheduled', None),
            "hs_v2_date_exited_qualifiedtobuy": val_dict.get('hs_v2_date_exited_qualifiedtobuy', None),
            "hs_v2_date_entered_qualifiedtobuy": val_dict.get('hs_v2_date_entered_qualifiedtobuy', None),
            "hs_v2_date_entered_decisionmakerboughtin": val_dict.get('hs_v2_date_entered_decisionmakerboughtin', None),
            "date_entered__demo_in_progress_": val_dict.get('date_entered__demo_in_progress_', None),
            "hs_v2_date_entered_contractsent": val_dict.get('hs_v2_date_entered_contractsent', None)
        }
        entities.append(entity)

    if entities:
        table.update_entities(entities=entities, table_name='hubspotdeal')
        logging.info(f'Inserted {len(entities)} deals to Azure Storage')
    else:
        logging.warning('No valid deals to update in Azure Storage')

def update_hubspot_owners(owners):
    logging.info('Inserting owners to Azure Storage')
    table = azureTableStorage.Table(config=config_obj)
    entities = []

    for owner in owners:
        updated_at = owner.updated_at.isoformat() if hasattr(owner, 'updated_at') and owner.updated_at else None
        created_at = owner.created_at.isoformat() if hasattr(owner, 'created_at') and owner.created_at else None

        entity = {
            "PartitionKey": "owners",
            "RowKey": str(getattr(owner, 'id', None)),
            "id": str(getattr(owner, 'id', None)),
            "email": escape_special_characters(getattr(owner, 'email', '')),
            "firstName": escape_special_characters(getattr(owner, 'first_name', '')),
            "lastName": escape_special_characters(getattr(owner, 'last_name', '')),
            "userId": getattr(owner, 'user_id', None),
            "createdAt": created_at,
            "updatedAt": updated_at,
            "archived": format_boolean(getattr(owner, 'archived', None)),
            "type": getattr(owner, 'type', None)
        }
        entities.append(entity)

    if entities:
        table.update_entities(entities=entities, table_name='hubspotowner')
        logging.info(f'Inserted {len(entities)} owners to Azure Storage')
    else:
        logging.warning('No valid owners to update in Azure Storage')