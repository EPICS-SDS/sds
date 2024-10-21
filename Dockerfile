FROM condaforge/miniforge3:24.7.1-2

RUN groupadd -r -g 10058 sds_group \
  && useradd --no-log-init -r -g sds_group -u 10057 sds-user

COPY src/environment.yml /app/environment.yml

# This variables are required to compile and link p4p properly in linux/arm64
ENV CFLAGS="-pthread"
ENV CXXFLAGS="-pthread"
ENV LFLAGS="-pthread"

RUN conda update -n base conda \
  && conda config --system --set channel_alias https://artifactory.esss.lu.se/artifactory/api/conda \
  && mamba env create -n sds -f /app/environment.yml \
  && conda clean -ay

COPY --chown=sds-user:sds_group src /app

# Make sure the /app directory is owned by sds-user
RUN chown -R sds-user:sds_group /app
WORKDIR /app

RUN mkdir -p static && wget -O static/swagger-ui-bundle.js https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js \
  && wget -O static/swagger-ui.css https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css \
  && wget -O static/swagger-ui-bundle.js.map https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js.map \
  && wget -O static/swagger-ui.css.map https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css.map \
  && wget -O static/redoc.standalone.js https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js

USER sds-user

# Fixes python not printing anything
ENV PYTHONUNBUFFERED=1

ENV PATH=/opt/conda/envs/sds/bin:$PATH
