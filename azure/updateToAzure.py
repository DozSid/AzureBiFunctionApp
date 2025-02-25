from typing_extensions import TypedDict
from datetime import datetime
from azure import azureTableStorage
import logging
import os
from pandas import isna
from typing import Optional , List
from hubspot.crm.companies import SimplePublicObjectWithAssociations

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

class HubspotDeal(TypedDict, total=False):
    PartitionKey: str
    RowKey: str
    hs_deal_id: int
    dealname: str
    dealstage: str
    pipeline: str
    amount: Optional[float]
    closedate: Optional[datetime]
    createdate: Optional[datetime]
    hs_lastmodifieddate: Optional[datetime]
    demo_start_date: Optional[datetime]
    demo_end_date: Optional[datetime]
    demo_installation_date: Optional[datetime]
    current_status: Optional[str]
    no__of_demo_days: Optional[int]
    no__of_devices: Optional[int]
    total_no__of_hospital_beds: Optional[int]
    total_no__of_prospective_dozee_beds__65__target_: Optional[int]
    zone: Optional[str]
    category_of_hospital: Optional[str]
    hubspot_owner_id: Optional[int]

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
    type: str

class HubspotEvent(TypedDict, total=False):
    PartitionKey: str
    RowKey: str
    hs_event_id: str
    event_name: str
    event_type: str
    hs_pipeline_stage: str
    hubspot_owner_id: Optional[int]
    purpose_of_the_event: str
    zone: str
    city: str
    number_of_attendees: Optional[int]
    speaker_name__if_applicable_: str
    total_event_roi: Optional[float]
    single_multi_hospital_event: str
    event_start_date: Optional[datetime]
    event_end_date: Optional[datetime]
    hs_createdate: Optional[datetime]
    updated_at: Optional[datetime]

class HubspotCompanies(TypedDict, total=False):
    PartitionKey: str
    RowKey: str
    companyid: int
    name: Optional[str]
    domain: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    archived: bool
    proposal_sent_date: Optional[datetime]
    msa_a_beds: Optional[int]
    msa_t_beds: Optional[int]
    installation_a_beds: Optional[int]
    installation_t_beds: Optional[int]
    final_target_month__msa_: Optional[str]
    final_target_month__installation_: Optional[str]
    final_target_year__msa_: Optional[int]
    final_target_year__installation_: Optional[int]

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
            "pipeline": escape_special_characters(val_dict.get('pipeline', '')),
            "dealname": escape_special_characters(val_dict.get('dealname', '')),
            "dealstage": escape_special_characters(val_dict.get('dealstage', '')),
            "amount": val_dict.get('amount', None),
            "closedate": val_dict.get('closedate', None),
            "createdate": val_dict.get('createdate', None),
            "hs_lastmodifieddate": val_dict.get('hs_lastmodifieddate', None),
            "demo_start_date": val_dict.get('demo_start_date', None),
            "demo_end_date": val_dict.get('demo_end_date', None),
            "demo_installation_date": val_dict.get('demo_installation_date', None),
            "current_status": val_dict.get('current_status', None),
            "no__of_demo_days": val_dict.get('no__of_demo_days', None),
            "no__of_devices": val_dict.get('no__of_devices', None),
            "total_no__of_hospital_beds": val_dict.get('total_no__of_hospital_beds', None),
            "total_no__of_prospective_dozee_beds__65__target_": val_dict.get('total_no__of_prospective_dozee_beds__65__target_', None),
            "zone": val_dict.get('zone', None),
            "category_of_hospital": val_dict.get('category_of_hospital', None),
            "hubspot_owner_id": val_dict.get('hubspot_owner_id', None)
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

def update_hubspot_events(events):
    logging.info('Inserting events to Azure Storage')
    table = azureTableStorage.Table(config=config_obj)
    entities = []

    for event in events:
        properties = event.get("properties", {}) 
        updated_at = event.get("updatedAt", None) 

        val_dict = {
            key: None if value in [None, "", "null"] else value
            for key, value in properties.items()
        }

        entity: HubspotEvent = {
            "PartitionKey": "events",
            "RowKey": val_dict.get('hs_object_id', ""),
            "hs_event_id": val_dict.get('hs_object_id', ""),
            "event_name": escape_special_characters(val_dict.get('event_name', "")),
            "event_type": escape_special_characters(val_dict.get('event_type', "")),
            "hs_pipeline_stage": escape_special_characters(val_dict.get('hs_pipeline_stage', "")),
            "hubspot_owner_id": val_dict.get('hubspot_owner_id', None),
            "purpose_of_the_event": escape_special_characters(val_dict.get('purpose_of_the_event', "")),
            "zone": escape_special_characters(val_dict.get('zone', "")),
            "city": escape_special_characters(val_dict.get('city', "")),
            "number_of_attendees": val_dict.get('number_of_attendees', None),
            "speaker_name__if_applicable_": escape_special_characters(val_dict.get('speaker_name__if_applicable_', "")),
            "total_event_roi": float(val_dict['total_event_roi']) if val_dict.get('total_event_roi') not in [None, ""] else None,  # Fix: Convert empty to None
            "single_multi_hospital_event": escape_special_characters(val_dict.get('single_multi_hospital_event', "")),
            "event_start_date": val_dict.get('event_start_date', None),
            "event_end_date": val_dict.get('event_end_date', None),
            "hs_createdate": val_dict.get('hs_createdate', None),
            "updated_at": updated_at
        }
        entities.append(entity)

    if entities:
        table.update_entities(entities=entities, table_name='hubspotevent')
        logging.info(f'Inserted {len(entities)} events to Azure Storage')
    else:
        logging.warning('No valid events to update in Azure Storage')

def update_hubspot_companies(companies: List[SimplePublicObjectWithAssociations]):
    logging.info('Inserting companies to Azure Storage')
    table = azureTableStorage.Table(config=config_obj)
    entities = []

    for company in companies:
        properties = company.properties
        company_id = properties.get('hs_object_id')
        name = properties.get('name')
        domain = properties.get('domain')
        created_at = company.created_at if hasattr(company, 'created_at') else None
        updated_at = company.updated_at if hasattr(company, 'updated_at') else None

        # Convert numeric fields to integers
        msa_a_beds = int(properties.get('msa_a_beds', 0)) if properties.get('msa_a_beds') else 0
        msa_t_beds = int(properties.get('msa_t_beds', 0)) if properties.get('msa_t_beds') else 0
        installation_a_beds = int(properties.get('installation_a_beds', 0)) if properties.get('installation_a_beds') else 0
        installation_t_beds = int(properties.get('installation_t_beds', 0)) if properties.get('installation_t_beds') else 0
        final_target_year__msa_ = int(properties.get('final_target_year__msa_', 0)) if properties.get('final_target_year__msa_') else 0
        final_target_year__installation_ = int(properties.get('final_target_year__installation_', 0)) if properties.get('final_target_year__installation_') else 0

        # Ensure date fields are in string format
        proposal_sent_date = properties.get('proposal_sent_date', '')

        # Format dates
        created_at_iso = created_at.isoformat() if created_at else ''
        updated_at_iso = updated_at.isoformat() if updated_at else ''

        # Construct the entity
        entity = {
            "PartitionKey": "companies",
            "RowKey": str(company_id),
            "companyid": str(company_id) if company_id else '',
            "name": str(name) if name else '',
            "domain": str(domain) if domain else '',
            "created_at": str(created_at_iso) if created_at_iso else '',
            "updated_at": str(updated_at_iso) if updated_at_iso else '',
            "archived": bool(company.archived) if hasattr(company, 'archived') else False,
            "proposal_sent_date": str(proposal_sent_date),
            "msa_a_beds": msa_a_beds,
            "msa_t_beds": msa_t_beds,
            "installation_a_beds": installation_a_beds,
            "installation_t_beds": installation_t_beds,
            "final_target_month__msa_": str(properties.get('final_target_month__msa_', '')),
            "final_target_month__installation_": str(properties.get('final_target_month__installation_', '')),
            "final_target_year__msa_": final_target_year__msa_,
            "final_target_year__installation_": final_target_year__installation_
        }
        entities.append(entity)

    if entities:
        table.update_entities(entities=entities, table_name='hubspotcompany')
        logging.info(f'Successfully inserted {len(entities)} companies to Azure Storage')
    else:
        logging.warning('No valid companies to update in Azure Storage')