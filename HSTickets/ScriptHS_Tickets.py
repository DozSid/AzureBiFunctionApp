import hubspot
from hubspot.crm.tickets import PublicObjectSearchRequest, ApiException
import logging
from datetime import datetime, timezone, timedelta

class Tickets:
    def __init__(self, access_token):
        self.access_token = access_token
        self.client = hubspot.Client.create(access_token=self.access_token)

    def fetch_all_tickets(self, pipeline="99048026", days_back=10):
        all_results = []
        after = None
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)

        # Format dates as ISO 8601 strings (HubSpot requires this format)
        start_date_iso = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_date_iso = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        properties = [
            "closed_date", "content", "createdate", "creator_name", "customer_code___erp",
            "defect", "hs_pipeline", "hs_pipeline_stage", "hs_primary_company_name",
            "hs_ticket_id", "hubspot_owner_id", "issue_type", "item_name___id", "product",
            "regulatory_reportable_incident", "si_jira_issue_id", "subject",
            "ticket_priority", "ticket_type", "tat", "tat_in_hours"
        ]

        while True:
            public_object_search_request = PublicObjectSearchRequest(
                limit=100,
                properties=properties,
                filter_groups=[
                    {
                        "filters": [
                            {"propertyName": "hs_pipeline", "value": pipeline, "operator": "EQ"},
                            {"propertyName": "hs_lastmodifieddate", "value": start_date_iso, "operator": "GTE"},
                            {"propertyName": "hs_lastmodifieddate", "value": end_date_iso, "operator": "LTE"},
                        ]
                    }
                ],
                after=after
            )

            try:
                api_response = self.client.crm.tickets.search_api.do_search(
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