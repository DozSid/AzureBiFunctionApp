import hubspot
from hubspot.crm.companies import PublicObjectSearchRequest, ApiException
import logging
from datetime import datetime, timezone, timedelta

class Companies:
    def __init__(self, access_token):
        self.access_token = access_token
        self.client = hubspot.Client.create(access_token=self.access_token)

    def fetch_all_companies(self, days_back=10):
        all_results = []
        after = None
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)

        start_date_iso = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_date_iso = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

        properties = [
            "name", "domain", "createdate", "hs_lastmodifieddate", "archived",
            "archived_at", "proposal_sent_date", "msa_a_beds", "msa_t_beds",
            "installation_a_beds", "installation_t_beds","final_target_month__msa_",
            "final_target_month__installation_"
        ]

        while True:
            public_object_search_request = PublicObjectSearchRequest(
                limit=100,
                properties=properties,
                filter_groups=[
                    {
                        "filters": [
                            {"propertyName": "hs_lastmodifieddate", "value": start_date_iso, "operator": "GTE"},
                            {"propertyName": "hs_lastmodifieddate", "value": end_date_iso, "operator": "LTE"},
                        ]
                    }
                ],
                after=after
            )

            try:
                api_response = self.client.crm.companies.search_api.do_search(
                    public_object_search_request=public_object_search_request
                )
                all_results.extend(api_response.results)

                if api_response.paging and api_response.paging.next and api_response.paging.next.after:
                    after = api_response.paging.next.after
                else:
                    break

            except ApiException as e:
                logging.error(f"Exception when calling search_api->do_search: {str(e)}")
                raise

        return all_results