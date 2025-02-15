import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi import HTTPException, Response
from tests.functional.service_loader import indexer_service

from esds.collector import api
from esds.collector.collector import Collector
from esds.collector.collector_manager import CollectorManager
from esds.collector.collector_status import CollectorBasicStatus, CollectorFullStatus
from esds.collector.config import settings
from esds.common.files import CollectorDefinition, Event
from esds.common.p4p_type import P4pType
from esds.common.schemas import CollectorBase

collector = Collector(
    name="test_collector",
    parent_path="/",
    event_code=1,
    pvs=["TEST:PV:1", "TEST:PV:2"],
    id="test_id",
    host="0.0.0.0",
)

acq_event = dict(
    timestamp=datetime.now(UTC),
    name="TestEvent",
    delay=0.0,
    code=0,
    evr="TestEVR",
)

beam_info = dict(
    mode="TestMode",
    state="ON",
    present="YES",
    len=2.86e-3,
    energy=2e9,
    dest="Target",
    curr=62.5e-3,
)

event = Event(
    pv_name="TEST:PV:3",
    value=1,
    type=None,
    timing_event_code=2,
    data_timestamp=datetime.now(UTC),
    sds_event_timestamp=datetime.now(UTC),
    cycle_id_timestamp=datetime.now(UTC),
    cycle_id=1,
    sds_event_cycle_id=1,
    attributes=dict(acq_event=acq_event, beam_info=beam_info),
    event_size=1,
)

NON_EXISTING = "non existing"


@pytest.mark.usefixtures("indexer_service")
class TestCollectorApi:
    @classmethod
    def setup_class(cls):
        cls.old_collectors = settings.collector_definitions
        settings.collector_definitions = Path("./collectors_api_test.json")

    @classmethod
    def teardown_class(cls):
        settings.collector_definitions = cls.old_collectors

    async def test_add_collector(self):
        cm = await CollectorManager.create([])
        await api.add_collector(
            collector_in=CollectorDefinition.model_validate(collector.model_dump()),
            start_collector=False,
        )

        assert collector.collector_id in cm.collectors.keys()

        await cm.close()
        await cm.join()

    async def test_add_collector_twice(self):
        cm = await CollectorManager.create([])

        await api.add_collector(
            collector_in=CollectorDefinition.model_validate(collector.model_dump()),
            start_collector=False,
        )

        assert collector.collector_id in cm.collectors.keys()

        try:
            await api.add_collector(
                collector_in=CollectorDefinition.model_validate(collector.model_dump()),
                start_collector=False,
            )
        except HTTPException:
            assert True
        else:
            assert False

        await cm.close()
        await cm.join()

    async def test_remove_collector(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )
        assert collector.collector_id in cm.collectors.keys()

        response = Response()
        await api.remove_collector(
            collector_id=collector.collector_id, response=response
        )

        assert collector.collector_id not in cm.collectors.keys()
        assert response.status_code == 200

        await cm.close()
        await cm.join()

    async def test_remove_not_existing_collector(self):
        cm = await CollectorManager.create([])

        response = Response()
        await api.remove_collector(
            collector_id=collector.collector_id, response=response
        )

        assert response.status_code == 404

        await cm.close()
        await cm.join()

    async def test_get_collectors(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )
        assert collector.collector_id in cm.collectors.keys()

        response = await api.get_collectors()

        assert isinstance(response, list)
        assert isinstance(response[0], CollectorBase)

        await cm.close()
        await cm.join()

    async def test_get_collector(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )
        assert collector.collector_id in cm.collectors.keys()

        response = await api.get_collector_with_collector_id(
            collector_id=collector.collector_id
        )

        assert isinstance(response, CollectorBase)

        await cm.close()
        await cm.join()

    async def test_get_non_existing_collector(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )
        assert collector.collector_id in cm.collectors.keys()

        try:
            await api.get_collector_with_collector_id(collector_id="not_existing")
        except HTTPException:
            assert True
        else:
            assert False

        await cm.close()
        await cm.join()

    async def test_autosave(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        response = await api.save_collectors_definition()

        assert (
            response
            == "Autosave is enabled. Collectors definition file already up-to-date."
        )

        await cm.close()
        await cm.join()

    async def test_save_collectors_definition(self):
        settings.autosave_collectors_definition = False

        collector_definitions = settings.collector_definitions.absolute()

        if os.path.exists(collector_definitions):
            os.remove(collector_definitions)

        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        response = await api.save_collectors_definition()

        assert response is None
        assert os.path.exists(
            collector_definitions
        ), f"Configuration file {collector_definitions} does not exist"

        settings.autosave_collectors_definition = True

        await cm.close()
        await cm.join()

    async def test_get_status(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        response = await api.get_status()

        assert isinstance(response[0], CollectorBasicStatus)
        assert isinstance(response, list)
        assert response[0].name == collector.name, response

        await cm.close()
        await cm.join()

    async def test_get_full_status(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        response = await api.get_full_status()

        assert isinstance(response[0], CollectorFullStatus)
        assert isinstance(response, list)
        assert response[0].name == collector.name

        await cm.close()
        await cm.join()

    async def test_get_status_with_collector_id(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )

        assert isinstance(response, CollectorFullStatus)
        assert response.collector_id == collector.collector_id

        await cm.close()
        await cm.join()

    async def test_get_status_with_wrong_id(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        try:
            await api.get_status_with_collector_id(collector_id=NON_EXISTING)
        except HTTPException:
            assert True
        else:
            assert False

        await cm.close()
        await cm.join()

    async def test_start_all_collectors(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert not response.running
        await api.start_all_collectors()

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert response.running

        await cm.close()
        await cm.join()

    async def test_start_all_collectors_twice(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert not response.running
        await api.start_all_collectors()

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert response.running

        await api.start_all_collectors()

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert response.running

        await cm.close()
        await cm.join()

    async def test_stop_all_collectors(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        await api.start_all_collectors()

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert response.running
        await api.stop_all_collectors()

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert not response.running

        await cm.close()
        await cm.join()

    async def test_stop_all_collectors_twice(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        await api.start_all_collectors()

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert response.running
        await api.stop_all_collectors()

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert not response.running
        await api.stop_all_collectors()

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert not response.running

        await cm.close()
        await cm.join()

    async def test_start_collector(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert not response.running
        await api.start_collector(collector_id=collector.collector_id)

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert response.running

        await cm.close()
        await cm.join()

    async def test_start_collector_twice(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert not response.running
        await api.start_collector(collector_id=collector.collector_id)

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert response.running
        await api.start_collector(collector_id=collector.collector_id)

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert response.running

        await cm.close()
        await cm.join()

    async def test_stop_collector(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        await api.start_collector(collector_id=collector.collector_id)

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert response.running
        await api.stop_collector(collector_id=collector.collector_id)

        response = await api.get_status_with_collector_id(
            collector_id=collector.collector_id
        )
        assert not response.running

        await cm.close()
        await cm.join()

    async def test_start_non_existing_collector(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        try:
            await api.start_collector(collector_id=NON_EXISTING)
        except HTTPException:
            assert True
        else:
            assert False

        await cm.close()
        await cm.join()

    async def test_stop_non_existing_collector(self):
        cm = await CollectorManager.create(
            [CollectorDefinition.model_validate(collector.model_dump())]
        )

        try:
            await api.stop_collector(collector_id=NON_EXISTING)
        except HTTPException:
            assert True
        else:
            assert False

        await cm.close()
        await cm.join()
