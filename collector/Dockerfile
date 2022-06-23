FROM continuumio/miniconda3:4.7.12

RUN groupadd -r -g 1000 csi \
  && useradd --no-log-init -r -g csi -u 1000 csi

COPY environment.yml /app/environment.yml

RUN conda update -n base conda \
  && conda config --system --set channel_alias https://artifactory.esss.lu.se/artifactory/api/conda \
  && conda config --system --set use_only_tar_bz2 true \
  && conda env create -n sds_collector -f /app/environment.yml \
  && conda clean -ay

COPY --chown=csi:csi . /app/collector

# Make sure the /app directory is owned by csi
RUN chown -R csi:csi /app
WORKDIR /app/collector

USER csi

ENV PATH=/opt/conda/envs/sds_collector/bin:$PATH

# Fixes python not printing anything
ENV PYTHONUNBUFFERED=1

CMD python -m app.main