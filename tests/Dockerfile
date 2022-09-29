FROM sds

USER root

RUN pip install pytest \
    && pip install pytest-cov \
    && pip install pytest-asyncio \
    && pip install coverage \
    && pip install aiofiles


RUN mkdir /data 


EXPOSE 8000 8001

CMD python -m pytest tests -v
