FROM sds

USER root

RUN pip install pytest

COPY --chown=csi:csi . /app/tests

USER csi

EXPOSE 8000

CMD pytest tests -v