FROM continuumio/miniconda3:latest

RUN groupadd -r -g 1000 csi \
  && useradd --no-log-init -r -g csi -u 1000 csi

COPY indexer/environment.yml /app/environment.yml

RUN conda update -n base conda \
  && conda config --system --set channel_alias https://artifactory.esss.lu.se/artifactory/api/conda \
  && conda config --system --set use_only_tar_bz2 true \
  && conda env create -n sds_indexer -f /app/environment.yml \
  && conda clean -ay

RUN conda install curl

COPY --chown=csi:csi indexer /app/
COPY --chown=csi:csi common /app/common

# Make sure the /app directory is owned by csi
RUN chown -R csi:csi /app
WORKDIR /app

USER csi

ENV PATH=/opt/conda/envs/sds_indexer/bin:$PATH

CMD uvicorn indexer:app --host 0.0.0.0 --port ${INDEXER_PORT:-"8000"}
