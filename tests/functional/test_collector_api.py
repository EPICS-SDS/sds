import os
from datetime import datetime
from pathlib import Path

import pytest
from fastapi import HTTPException, Response
from sds.collector import api
from sds.collector.collector import Collector
from sds.collector.collector_manager import CollectorManager
from sds.collector.collector_status import CollectorBasicStatus, CollectorFullStatus
from sds.collector.config import settings
from sds.common.files import AcqEvent, AcqInfo, BeamInfo, CollectorDefinition, Event
from sds.common.schemas import CollectorBase
from tests.functional.service_loader import indexer_service

collector = Collector(
    name="test_collector",
    event_name="test_event",
    event_code=1,
    pvs=["TEST:PV:1", "TEST:PV:2"],
    id="test_id",
    host="0.0.0.0",
)

acq_info = AcqInfo(
    acq_type="",
    id=0,
)

acq_event = AcqEvent(
    timestamp=datetime.utcnow(),
    name="TestEvent",
    delay=0.0,
    code=0,
    evr="TestEVR",
)

beam_info = BeamInfo(
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
    timing_event_code=2,
    data_timestamp=datetime.utcnow(),
    sds_event_timestamp=datetime.utcnow(),
    pulse_id=1,
    sds_event_pulse_id=1,
    acq_info=acq_info,
    acq_event=acq_event,
    beam_info=beam_info,
)

NON_EXISTING = "non existing"


class TestCollectorApi:
    @pytest.fixture(autouse=True)
    def _start_collector_service(self, indexer_service):
        pass

    @classmethod
    def setup_class(cls):
        cls.old_collectors = settings.collector_definitions
        settings.collector_definitions = Path("./collectors_api_test.json")

    @classmethod
    def teardown_class(cls):
        settings.collector_definitions = cls.old_collectors

    @pytest.mark.asyncio
    async def test_add_collector(self):
        cm = await CollectorManager.create([])
        await api.add_collector(
            collector_in=CollectorDefinition.parse_obj(collector), start_collector=False
        )

        assert collector.name in cm.collectors.keys()

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_add_collector_twice(self):
        cm = await CollectorManager.create([])

        await api.add_collector(
            collector_in=CollectorDefinition.parse_obj(collector), start_collector=False
        )

        assert collector.name in cm.collectors.keys()

        try:
            await api.add_collector(
                collector_in=CollectorDefinition.parse_obj(collector),
                start_collector=False,
            )
        except HTTPException:
            assert True
        else:
            assert False

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_remove_collector(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])
        assert collector.name in cm.collectors.keys()

        response = Response()
        await api.remove_collector(name=collector.name, response=response)

        assert collector.name not in cm.collectors.keys()
        assert response.status_code == 200

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_remove_not_existing_collector(self):
        cm = await CollectorManager.create([])

        response = Response()
        await api.remove_collector(name=collector.name, response=response)

        assert response.status_code == 404

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_get_collectors(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])
        assert collector.name in cm.collectors.keys()

        response = await api.get_collectors()

        assert isinstance(response, list)
        assert isinstance(response[0], CollectorBase)

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_get_collector(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])
        assert collector.name in cm.collectors.keys()

        response = await api.get_collector_with_name(name=collector.name)

        assert isinstance(response, CollectorBase)

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_get_non_existing_collector(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])
        assert collector.name in cm.collectors.keys()

        try:
            await api.get_collector_with_name(name="not_existing")
        except HTTPException:
            assert True
        else:
            assert False

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_autosave(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        response = await api.save_collectors_definition()

        assert (
            response
            == "Autosave is enabled. Collectors definition file already up-to-date."
        )

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_save_collectors_definition(self):
        settings.autosave_collectors_definition = False

        collector_definitions = settings.collector_definitions.absolute()

        if os.path.exists(collector_definitions):
            os.remove(collector_definitions)

        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        response = await api.save_collectors_definition()

        assert response is None
        assert os.path.exists(
            collector_definitions
        ), f"Configuration file {collector_definitions} does not exist"

        settings.autosave_collectors_definition = True

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_get_status(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        response = await api.get_status()

        assert isinstance(response[0], CollectorBasicStatus)
        assert isinstance(response, list)
        assert response[0].name == collector.name, response

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_get_full_status(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        response = await api.get_full_status()

        assert isinstance(response[0], CollectorFullStatus)
        assert isinstance(response, list)
        assert response[0].name == collector.name

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_get_status_with_name(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        response = await api.get_status_with_name(name=collector.name)

        assert isinstance(response, CollectorFullStatus)
        assert response.name == collector.name

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_get_status_with_wrong_name(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        try:
            await api.get_status_with_name(name=NON_EXISTING)
        except HTTPException:
            assert True
        else:
            assert False

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_start_all_collectors(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        response = await api.get_status_with_name(name=collector.name)
        assert not response.running
        await api.start_all_collectors()

        response = await api.get_status_with_name(name=collector.name)
        assert response.running

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_start_all_collectors_twice(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        response = await api.get_status_with_name(name=collector.name)
        assert not response.running
        await api.start_all_collectors()

        response = await api.get_status_with_name(name=collector.name)
        assert response.running

        await api.start_all_collectors()

        response = await api.get_status_with_name(name=collector.name)
        assert response.running

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_stop_all_collectors(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        await api.start_all_collectors()

        response = await api.get_status_with_name(name=collector.name)
        assert response.running
        await api.stop_all_collectors()

        response = await api.get_status_with_name(name=collector.name)
        assert not response.running

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_stop_all_collectors_twice(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        await api.start_all_collectors()

        response = await api.get_status_with_name(name=collector.name)
        assert response.running
        await api.stop_all_collectors()

        response = await api.get_status_with_name(name=collector.name)
        assert not response.running
        await api.stop_all_collectors()

        response = await api.get_status_with_name(name=collector.name)
        assert not response.running

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_start_collector(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        response = await api.get_status_with_name(name=collector.name)
        assert not response.running
        await api.start_collector(name=collector.name)

        response = await api.get_status_with_name(name=collector.name)
        assert response.running

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_start_collector_twice(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        response = await api.get_status_with_name(name=collector.name)
        assert not response.running
        await api.start_collector(name=collector.name)

        response = await api.get_status_with_name(name=collector.name)
        assert response.running
        await api.start_collector(name=collector.name)

        response = await api.get_status_with_name(name=collector.name)
        assert response.running

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_stop_collector(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        await api.start_collector(name=collector.name)

        response = await api.get_status_with_name(name=collector.name)
        assert response.running
        await api.stop_collector(name=collector.name)

        response = await api.get_status_with_name(name=collector.name)
        assert not response.running

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_start_non_existing_collector(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        try:
            await api.start_collector(name=NON_EXISTING)
        except HTTPException:
            assert True
        else:
            assert False

        await cm.close()
        await cm.join()

    @pytest.mark.asyncio
    async def test_stop_non_existing_collector(self):
        cm = await CollectorManager.create([CollectorDefinition.parse_obj(collector)])

        try:
            await api.stop_collector(name=NON_EXISTING)
        except HTTPException:
            assert True
        else:
            assert False

        await cm.close()
        await cm.join()
