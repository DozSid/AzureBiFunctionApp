import hubspot
from hubspot.crm.tickets import PublicObjectSearchRequest, ApiException
import logging
from datetime import datetime, timezone, timedelta

class Tickets():
    def __init__(self, AccessToken):
        self.access_token = AccessToken

    def fetch_all_tickets(self):
        all_results = [] 
        after = None
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=10)

        # Format the dates as ISO 8601 strings
        start_date_iso = start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')  # Example: '2025-01-08T00:00:00.000Z'
        # end_date_iso = end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')  # Example: '2025-01-18T00:00:00.000Z'


        client = hubspot.Client.create(access_token=self.access_token)
        while True:
            public_object_search_request = PublicObjectSearchRequest(
                limit=100,
                properties=["closed_date", "content", "createdate",
                            "creator_name", "customer_code___erp", "defect", "hs_pipeline",
                            "hs_pipeline_stage", "hs_primary_company_name", "hs_ticket_id",
                            "hubspot_owner_id", "issue_type", "item_name___id", "product",
                            "regulatory_reportable_incident", "si_jira_issue_id", "subject",
                            "ticket_priority", "ticket_type", "tat", "tat_in_hours"],
                filter_groups=[
                    {"filters": [
                        {"propertyName": "hs_pipeline", "value": "99048026", "operator": "EQ"},
                        {
                            "propertyName": "hs_lastmodifieddate", 
                            "value": start_date_iso,
                            "operator": "GTE"
                        },
                        # {
                        #     "propertyName": "hs_lastmodifieddate",
                        #     "value": end_date_iso,
                        #     "operator": "LTE"
                        # }
                    ]}
                ],
                after=after
            )

            try:
                api_response = client.crm.tickets.search_api.do_search(
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