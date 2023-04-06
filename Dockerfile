FROM continuumio/miniconda3:22.11.1

RUN groupadd -r -g 10058 sds_group \
  && useradd --no-log-init -r -g sds_group -u 10057 sds-user

COPY src/environment.yml /app/environment.yml

RUN conda update -n base conda \
  && conda config --system --set channel_alias https://artifactory.esss.lu.se/artifactory/api/conda \
  && conda env create -n sds -f /app/environment.yml \
  && conda clean -ay

COPY --chown=sds-user:sds_group src /app

# Make sure the /app directory is owned by sds-user
RUN chown -R sds-user:sds_group /app
WORKDIR /app

USER sds-user

# Fixes python not printing anything
ENV PYTHONUNBUFFERED=1

ENV PATH=/opt/conda/envs/sds/bin:$PATH
