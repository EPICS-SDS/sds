FROM continuumio/miniconda3:4.7.12

RUN groupadd -r -g 1000 csi \
  && useradd --no-log-init -r -g csi -u 1000 csi

COPY collector/environment.yml /app/environment.yml

RUN conda update -n base conda \
  && conda config --system --set channel_alias https://artifactory.esss.lu.se/artifactory/api/conda \
  && conda config --system --set use_only_tar_bz2 true \
  && conda env create -n sds_collector -f /app/environment.yml \
  && conda clean -ay

COPY --chown=csi:csi collector /app/collector
COPY --chown=csi:csi common /app/common

# Make sure the /app directory is owned by csi
RUN chown -R csi:csi /app
WORKDIR /app/collector

USER csi

ENV PATH=/opt/conda/envs/sds_collector/bin:$PATH

CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-"8000"}