FROM sds

USER root

RUN pip install pytest \
    && pip install pytest-cov \
    && pip install pytest-asyncio \
    && pip install coverage \
    && pip install aiofiles

COPY --chown=csi:csi . /app/tests

RUN mkdir /data && chown csi:csi /data

USER csi

EXPOSE 8000 8001

CMD python -m pytest tests -v
