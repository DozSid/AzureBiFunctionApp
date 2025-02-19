from HSTickets import ScriptHS_Tickets
from azure import updateToAzure
import os
import logging

def getTicketsInfoFromHB():
    tickets = ScriptHS_Tickets.Tickets(os.environ.get('HB_Access_Token'))
    fetchtickets = (tickets.fetch_all_tickets())
    logging.info(f"Fetched {len(fetchtickets)} tickets from hubspot")
    updateToAzure.update_hubspot_tickets(fetchtickets)