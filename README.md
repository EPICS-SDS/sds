# Synchronous Data Service

The Synchronous Data Service (SDS) is an EPICS service that allows archiving PVs in a synchronous fashion.

Acquisitions are triggered by the timing system, and the list of PVs to be archived depends on the event generated by the timing.

Data is stored in HDF5 following the NeXus format.