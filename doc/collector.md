# SDS Collector

The collector is a service that monitors a set of PVs and generate NeXus files according to its configuration.

The configuration is a text file in JSON format that contains with the following structure:
```json
[
    {
        "name": "name0",
        "event_name": "type0",
        "event_code": 0,
        "pvs": [
            "pv0",
            "pv1"
        ],
        "reductionFactor": 14,
        "reductionTime": 3600
    },
    {
        "name": "name1",
        "event_name": "type0",
        "event_code": 0,
        "pvs": [
            "pv1",
            "pv2"
        ]
    },
    {
        "name": "name2",
        "event_name": "type1",
        "event_code": 1,
        "pvs": [
            "pv1",
            "pv2"
        ]
    }
]
```

As can be seen in the example above, the configuration defines sets of PVs associated with a timing event. The same timing event can be used to store different PVs, and the same PVs can be monitored for different events.

The reduction parameters are optional. If they are not defined, the data is stored forever. If they are defined, the data is reduced by the amount defined by `reductionFactor` after the an amount of time from the acquisition defined by `reductionTime` in seconds. The `reductionFactor` is an integer N that defines that only every N-th acquisition is kept forever in the SDS storage.

The collector behaves according to the following flow chart:

![](collector.svg)

The collector manager starts monitoring the PVs of all the collectors. When a value is received by the manager it forwards the value to the appropriate collectors.

If the collector has no current dataset it creates a new one, and adds the PV value to it. Each subsequent value will be added to the dataset until it is complete or the timeout elapses.

When the dataset is complete or timed out, it is written to an HDF5 file and its metadata is uploaded to the indexer service. The collector is then ready to start again with a new dataset.
