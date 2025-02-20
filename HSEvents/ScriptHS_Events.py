import requests
import logging

class Events:
    def __init__(self, access_token):
        self.access_token = access_token
        self.url = "https://api.hubapi.com/crm/v3/objects/2-31844479/search"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }

    def fetch_all_events(self):
        all_results = []
        after = None

        while True:
            payload = {
                "limit": 100,
                "properties": ["event_name", "event_type", "hs_pipeline_stage", "hubspot_owner_id", 
                               "purpose_of_the_event", "zone", "city", "number_of_attendees", 
                               "speaker_name__if_applicable_", "total_event_roi", "single_multi_hospital_event",
                               "event_start_date", "event_end_date", "hs_createdate"],
                "sorts": [
                    {
                        "propertyName": "hs_createdate",
                        "direction": "DESCENDING"
                    }
                ],
                "after": after
            }

            response = requests.post(self.url, json=payload, headers=self.headers)

            if response.status_code != 200:
                logging.error(f"Error {response.status_code}: {response.json()}")
                break

            data = response.json()
            if not data.get("results"):
                break

            all_results.extend(data["results"])
            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                break

        return all_results