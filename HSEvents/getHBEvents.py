from HSEvents import ScriptHS_Events
from azure import updateToAzure
import os
import logging

def getEventsInfoFromHB():
    events = ScriptHS_Events.Events(os.environ.get('HB_Access_Token'))
    fetchevents = (events.fetch_all_events())
    logging.info(f"Fetched {len(fetchevents)} events from HubSpot")
    updateToAzure.update_hubspot_events(fetchevents)