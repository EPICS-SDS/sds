import logging

from common.db import settings
from common.db.base_class import Base
from common.db.connection import get_connection
from common.db.fields import Date, Integer, Keyword
from common.db.utils import UpdateRequiredException, check_dict_for_updated_entries
from elasticsearch import AsyncElasticsearch, NotFoundError

logger = logging.getLogger("sds_common")

DATASET_POLICY_NAME = "dataset_policy"
DATASET_MAPPING_TEMPLATE = "dataset_mapping"
DATASET_INDEX_SETTINGS_TEMPLATE = "dataset_index_settings"
COMPONENT_TEMPLATE_STR = "component_templates"
DATASET_INDEX_TEMPLATE = "dataset_template"


class Dataset(Base):
    collector_id: Keyword
    trigger_date: Date
    trigger_pulse_id: Integer
    path: Keyword
    timestamp: Date

    class Config:
        fields = {"timestamp": "@timestamp"}

    class Index:
        name = "dataset"

    @classmethod
    async def init(cls):
        """
        Datasets are stored in a data stream that automatically handles creation of new indices.
        This method initializes or updates (if needed) the Index Lifecicle Management policies
        and it creates the component templates and the index template that generetes the data stream.
        """
        async with get_connection() as es:
            # Set or update the ILM policy
            await cls.init_ilm_policy(es)

            # Create or update component templates
            await cls.init_dataset_mapping_template(es)
            await cls.init_dataset_index_settings(es)

            # Create or update index template
            await cls.init_index_template(es)

    @classmethod
    async def init_ilm_policy(cls, es: AsyncElasticsearch):
        ilm_policy = {
            "_meta": {
                "description": "ILM policy for dataset data stream",
            },
            "phases": {
                "hot": {
                    "actions": {
                        "rollover": {
                            "max_docs": str(settings.ilm_policy_max_docs),
                            "max_primary_shard_size": settings.ilm_policy_max_primary_shard_size,
                        }
                    },
                },
                "warm": {
                    "actions": {
                        "shrink": {"number_of_shards": 1},
                        "forcemerge": {"max_num_segments": 1},
                    },
                },
            },
        }

        try:
            current_policy = await es.ilm.get_lifecycle(name=DATASET_POLICY_NAME)
            check_dict_for_updated_entries(
                current_policy[DATASET_POLICY_NAME]["policy"], ilm_policy
            )
        except (NotFoundError, UpdateRequiredException):
            logger.info("ES dataset_policy ILM policy created or updated")
            await es.ilm.put_lifecycle(name=DATASET_POLICY_NAME, policy=ilm_policy)

    @classmethod
    async def init_dataset_mapping_template(cls, es: AsyncElasticsearch):
        try:
            dataset_mapping_template = await es.cluster.get_component_template(
                name=DATASET_MAPPING_TEMPLATE
            )
            dataset_mapping_template = dataset_mapping_template.get(
                COMPONENT_TEMPLATE_STR, None
            )
            if dataset_mapping_template is None:
                raise UpdateRequiredException
            for template in dataset_mapping_template:
                if template["name"] == DATASET_MAPPING_TEMPLATE:
                    try:
                        check_dict_for_updated_entries(
                            template["component_template"]["template"]["mappings"],
                            cls.mappings(),
                        )
                    except NameError:
                        raise UpdateRequiredException
                else:
                    raise UpdateRequiredException
        except (NotFoundError, UpdateRequiredException):
            logger.info("ES dataset_mapping template created or updated")
            await es.cluster.put_component_template(
                name=DATASET_MAPPING_TEMPLATE, template={"mappings": cls.mappings()}
            )

    @classmethod
    async def init_dataset_index_settings(cls, es: AsyncElasticsearch):
        try:
            dataset_index_settings = await es.cluster.get_component_template(
                name=DATASET_INDEX_SETTINGS_TEMPLATE
            )
            dataset_index_settings = dataset_index_settings.get(
                COMPONENT_TEMPLATE_STR, None
            )
            if dataset_index_settings is None:
                raise UpdateRequiredException
            for template in dataset_index_settings:
                if template["name"] == DATASET_INDEX_SETTINGS_TEMPLATE:
                    try:
                        if (
                            template["component_template"]["template"]["settings"][
                                "index"
                            ]["lifecycle"]["name"]
                            != DATASET_POLICY_NAME
                        ):
                            raise UpdateRequiredException
                    except NameError:
                        raise UpdateRequiredException
                else:
                    raise UpdateRequiredException
        except (NotFoundError, UpdateRequiredException):
            logger.info("ES dataset_index_settings template created or updated")
            await es.cluster.put_component_template(
                name=DATASET_INDEX_SETTINGS_TEMPLATE,
                template={"settings": {"index.lifecycle.name": DATASET_POLICY_NAME}},
            )

    @classmethod
    async def init_index_template(cls, es: AsyncElasticsearch):
        dataset_index_template = {
            "composed_of": [DATASET_MAPPING_TEMPLATE, DATASET_INDEX_SETTINGS_TEMPLATE],
            "data_stream": {},
            "priority": 500,
            "index_patterns": ["dataset*"],
            "_meta": {"description": "Template for the dataset schema"},
        }

        try:
            existing_dataset_index_template = await es.indices.get_index_template(
                name=DATASET_INDEX_TEMPLATE
            )
            existing_dataset_index_template = existing_dataset_index_template.get(
                "index_templates", None
            )
            if existing_dataset_index_template is None:
                raise UpdateRequiredException
            for template in existing_dataset_index_template:
                if template["name"] == DATASET_INDEX_TEMPLATE:
                    if template["index_template"] is None:
                        raise UpdateRequiredException
                    check_dict_for_updated_entries(
                        template["index_template"],
                        dataset_index_template,
                    )
                else:
                    raise UpdateRequiredException
        except (NotFoundError, UpdateRequiredException):
            logger.info("ES dataset_template template created or updated")
            await es.indices.put_index_template(
                name=DATASET_INDEX_TEMPLATE,
                **dataset_index_template,
            )
