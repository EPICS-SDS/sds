# SDS Collector

The collector is a service that monitors a set of PVs and generate NeXus files according to its configuration.

The configuration is a text file in JSON format that contains with the following structure:

    [
    {
        "name": "name0",
        "eventType": "type0",
        "pvs": [
            "pv0",
            "pv1"
        ]
    },
    {
        "name": "name1",
        "eventType": "type0",
        "pvs": [
            "pv1",
            "pv2"
        ]
    },
    {
        "name": "name2",
        "eventType": "type1",
        "pvs": [
            "pv1",
            "pv2"
        ]
    }
    ]

As can be seen in the example above, the configuration defines sets of PVs associated with a timing event. The same timing event can be used to store different PVs, and the same PVs can be monitored for different events.

The collector behaves according to the following flow chart:

![](collector-epics.svg)

The collector monitors all the PVs from the configuration, and after each update it checks the event type. If this is the first updated PV of the list, it creates a new file for that event and adds the data to it. It also sets a timer to finish the measurement in case no data is received from other PVs.
When an update is received for any other PV for the event that is already being acquired, data is added to the existing file.
Finally, if updates for all the PVs associated to the event are received, the file is send to storage and the metadata is saved in the database.

In case any PVs don't update after an event, a timeout will trigger after a while. Then the measurement will be considered completed and the data is finally stored.
![](collector-timeout.svg)

The acquisition of several simultaneous events is also possible, and the same PV updated after the same event can be saved in several files.
