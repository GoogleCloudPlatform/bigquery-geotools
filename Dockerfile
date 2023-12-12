# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM docker.osgeo.org/geoserver:2.21.1

ENV GEOSERVER_VERSION=2.21.1
ENV MNT_DIR /opt/geoserver_data

RUN apt update -y && apt install tini nfs-kernel-server nfs-common maven unzip -y && apt-get clean

COPY . .

RUN chmod +x filestore_run.sh

# install bigquery-geotools driver
RUN wget https://console.cloud.google.com/storage/browser/foci-geotools/zipfile-gt-bigquery.zip\
  && unzip zipfile-gt-bigquery.zip \
  && cp *.jar $GEOSERVER_LIB_DIR

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["./filestore_run.sh"]