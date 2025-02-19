from HSDeals import ScriptHS_Deals
from azure import updateToAzure
import os
import logging

def getDealsInfoFromHB():
    deals = ScriptHS_Deals.Deals(os.environ.get('HB_Access_Token'))
    fetchdeals = (deals.fetch_all_deals())
    logging.info(f"Fetched {len(fetchdeals)} deals from HubSpot")
    updateToAzure.update_hubspot_deals(fetchdeals)