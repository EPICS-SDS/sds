FROM sds

USER root

RUN pip install pytest

COPY --chown=csi:csi . /app/tests

USER csi

CMD pytest tests -v