import logging
import azure.functions as func
from HSTickets import getHBTickets
from HSDeals import getHBDeals
from HSOwners import getHBOwners
from HSEvents import getHBEvents

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 1 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def HubSpot(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    getHBTickets.getTicketsInfoFromHB()
    getHBDeals.getDealsInfoFromHB()
    getHBOwners.getOwnersInfoFromHB()
    getHBEvents.getEventsInfoFromHB()

    logging.info('Python timer trigger function executed.')