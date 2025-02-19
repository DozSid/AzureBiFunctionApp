from HSOwners import ScriptHS_Owners
from azure import updateToAzure
import os
import logging

def getOwnersInfoFromHB():
    owners = ScriptHS_Owners.Owners(os.environ.get('HB_Access_Token'))
    fetchowners = (owners.fetch_all_owners())
    logging.info(f"Fetched {len(fetchowners)} owners from HubSpot")
    updateToAzure.update_hubspot_owners(fetchowners)