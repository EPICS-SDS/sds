## API

- `/collectors` GET operation to retrieve a list of collectors matching the parameters
  - Parameters
    - `ids` (optional): a list of IDs to match 
    - `name` (optional): a string with the name.
    - `event_name` (optional): a string with the event name as defined in the timing system.
    - `event_code` (optional): an integer with the event code as defined in the timing system.
    - `pvs` (optional): a list of PVs.
  - Response
    
    Array of JSON documents of the collectors that match the parameters (HTTP 200)

- `/collectors/{id}` GET operation to retrieve a collector with the given ID
    - Response
        
        JSON document of the collector (HTTP 200) or HTTP 404 if there is no collector with the given ID

- `/datasets` GET operation to retrieve a list of datasets matching the parameters
  - Parameters
    - `collector_id` (optional): a list of collector IDs
    - `end` (optional): an [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) to filter out any `trigger_timestamp`s after this.
    - `start` (optional): an [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) date to filter out any `trigger_timestamp`s before this.
    - `end` (optional): an [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) date to filter out any `trigger_timestamp`s after this.
    - `trigger_pulse_id_start` (optional): a minimum integer pulse ID.
    - `trigger_pulse_id_end` (optional): a maximum integer pulse ID.
  - Response
    
    Array of JSON documents of the datasets that match the parameters (HTTP 200)

- `/datasets/{id}` GET operation to retrieve a dataset with the given ID
    - Response
        
        JSON document of the dataset (HTTP 200) or HTTP 404 if there is no dataset with the given ID

- `/datasets/{id}/file` GET operation to retrieve a dataset file with the given ID
    - Response
        
        The NeXus file (HTTP 200) or HTTP 404 if there is no dataset with the given ID