from HSCompanies import ScriptHS_Companies
from azure import updateToAzure
import os
import logging

def getCompaniesInfoFromHB():
    owners = ScriptHS_Companies.Companies(os.environ.get('HB_Access_Token'))
    fetchcompanies = (owners.fetch_all_companies())
    logging.info(f"Fetched {len(fetchcompanies)} companies from HubSpot")
    updateToAzure.update_hubspot_companies(fetchcompanies)