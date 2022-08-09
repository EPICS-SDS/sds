FROM sds

USER root

RUN pip install pytest \
    && pip install pytest-cov

COPY --chown=csi:csi . /app/tests

USER csi

EXPOSE 8000

CMD python -m pytest tests -v
