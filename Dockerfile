FROM docker.io/vaporio/python:3.8 as builder

# ARG BRANCH=master
# ARG ORG=vapor-ware

# ARG REQUIREMENTS_URL=https://raw.githubusercontent.com/$ORG/prophetess/$BRANCH/requirements.txt
# ADD ${REQUIREMENTS_URL} requirements.txt

COPY . /src/prophetess

ADD requirements.txt /requirements.txt

WORKDIR /build

RUN pip install --prefix=/build -r /requirements.txt --no-warn-script-location \
      && rm -rf /root/.cache

FROM docker.io/vaporio/python:3.8-slim
COPY --from=builder /build /usr/local
COPY --from=builder /src/prophetess /src

WORKDIR /src

RUN python setup.py install

ARG BUILD_DATE
ARG BUILD_VERSION
ARG VCS_REF

LABEL org.label-schema.schema-version="1.0" \
      org.label-schema.vendor="Vapor IO" \
      org.label-schema.name="vaporio/prophetess" \
      org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.vcs-url="https://github.com/vapor-ware/prophetess" \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vendor="Vapor IO" \
      org.label-schema.version=$BUILD_VERSION \
      maintainer="marco@vapor.io" \
      scr_url="$URL"

CMD ["python", "prophetess"]
