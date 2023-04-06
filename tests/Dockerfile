FROM sds

USER root

RUN pip install pytest \
    && pip install pytest-cov \
    && pip install pytest-asyncio \
    && pip install coverage \
    && pip install aiofiles


COPY --chown=sds-user:sds_group test_ioc /app/test_ioc

RUN mkdir /data 


EXPOSE 8000 8001

CMD python -m pytest tests -v
