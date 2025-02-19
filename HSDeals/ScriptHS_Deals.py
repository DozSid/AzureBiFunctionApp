import hubspot
from hubspot.crm.deals import PublicObjectSearchRequest, ApiException
import logging
from datetime import datetime, timezone, timedelta

class Deals:
    def __init__(self, access_token):
        self.access_token = access_token
        self.client = hubspot.Client.create(access_token=self.access_token)

    def fetch_all_deals(self, pipeline="default", days_back=10):
        all_results = []
        after = None
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)

        # Format dates as ISO 8601 strings (HubSpot requires this format)
        start_date_iso = start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_date_iso = end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        properties = [
            "dealname", "dealstage", "dealtype", "pipeline", "amount", 
            "closedate", "createdate", "hs_lastmodifieddate",
            "current_status", "hubspot_owner_id",
            "installation_a_beds", "hs_v2_date_entered_176875376",
            "hs_v2_date_entered_appointmentscheduled", "hs_v2_date_exited_appointmentscheduled",
            "hs_v2_date_exited_qualifiedtobuy", "hs_v2_date_entered_qualifiedtobuy",
            "hs_v2_date_entered_decisionmakerboughtin", "date_entered__demo_in_progress_",
            "hs_v2_date_entered_contractsent"
        ]

        while True:
            public_object_search_request = PublicObjectSearchRequest(
                limit=100,
                properties=properties,
                filter_groups=[
                    {
                        "filters": [
                            {"propertyName": "pipeline", "value": pipeline, "operator": "EQ"},
                            {"propertyName": "hs_lastmodifieddate", "value": start_date_iso, "operator": "GTE"},
                            {"propertyName": "hs_lastmodifieddate", "value": end_date_iso, "operator": "LTE"},
                        ]
                    }
                ],
                after=after
            )

            try:
                api_response = self.client.crm.deals.search_api.do_search(
                    public_object_search_request=public_object_search_request
                )
                all_results.extend(api_response.results)

                # Check for pagination
                if api_response.paging and api_response.paging.next and api_response.paging.next.after:
                    after = api_response.paging.next.after
                else:
                    break  # Exit loop if no more pages

            except ApiException as e:
                logging.error(f"Exception when calling search_api->do_search: {str(e)}")
                raise  # Re-raise the exception to handle it at a higher level

        return all_results