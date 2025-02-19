import hubspot
from hubspot.crm.deals import PublicObjectSearchRequest, ApiException
import logging
from datetime import datetime, timezone, timedelta

class Deals:
    def __init__(self, access_token):  # changed constructor to use lowercase access_token
        self.access_token = access_token

    def fetch_all_deals(self):
        all_results = []
        after = None
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=10)

        # Format dates as ISO 8601 strings (HubSpot requires this format)
        start_date_iso = start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        client = hubspot.Client.create(access_token=self.access_token)
        
        while True:
            public_object_search_request = PublicObjectSearchRequest(
                limit=100,
                properties=["dealname", "dealstage","dealtype", "pipeline", "amount", 
                            "closedate", "createdate", "hs_lastmodifieddate",
                            "current_status","hubspot_owner_id",
                            "installation_a_beds","hs_v2_date_entered_176875376",
                            "hs_v2_date_entered_appointmentscheduled","hs_v2_date_exited_appointmentscheduled",
                            "hs_v2_date_exited_qualifiedtobuy","hs_v2_date_entered_qualifiedtobuy",
                            "hs_v2_date_entered_decisionmakerboughtin","date_entered__demo_in_progress_",
                            "hs_v2_date_entered_contractsent"
                            ],
                filter_groups=[
                    {
                        "filters": [
                            {"propertyName": "pipeline", "value": "default", "operator": "EQ"},
                            {"propertyName": "hs_lastmodifieddate", "value": start_date_iso, "operator": "GTE"},
                        ]
                    }
                ],
                after=after
            )

            try:
                api_response = client.crm.deals.search_api.do_search(
                    public_object_search_request=public_object_search_request)
                all_results.extend(api_response.results)

                if api_response.paging and hasattr(api_response.paging, 'next') and api_response.paging.next:
                    after = api_response.paging.next.after
                else:
                    break

            except ApiException as e:
                logging.error(f"Exception when calling search_api->do_search: {str(e)}")
                break

        return all_results
