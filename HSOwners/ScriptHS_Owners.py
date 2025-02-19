import hubspot
from hubspot.crm.owners import ApiException
import logging

class Owners:
    def __init__(self, access_token):
        self.access_token = access_token
        self.client = hubspot.Client.create(access_token=self.access_token)

    def fetch_all_owners(self):
        all_results = []
        after = None

        while True:
            try:
                api_response = self.client.crm.owners.owners_api.get_page(after=after)
                all_results.extend(api_response.results)

                if api_response.paging and api_response.paging.next and api_response.paging.next.after:
                    after = api_response.paging.next.after
                else:
                    break

            except ApiException as e:
                logging.error(f"Exception when calling owners_api->get_page: {str(e)}")
                break

        return all_results