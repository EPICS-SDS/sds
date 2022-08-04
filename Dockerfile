FROM continuumio/miniconda3:4.12.0

RUN groupadd -r -g 1000 csi \
  && useradd --no-log-init -r -g csi -u 1000 csi

COPY app/environment.yml /app/environment.yml

RUN conda update -n base conda \
  && conda config --system --set channel_alias https://artifactory.esss.lu.se/artifactory/api/conda \
  && conda config --system --set use_only_tar_bz2 true \
  && conda env create -n sds -f /app/environment.yml \
  && conda clean -ay

COPY --chown=csi:csi app /app

# Make sure the /app directory is owned by csi
RUN chown -R csi:csi /app
WORKDIR /app

USER csi

# Fixes python not printing anything
ENV PYTHONUNBUFFERED=1

ENV PATH=/opt/conda/envs/sds/bin:$PATH