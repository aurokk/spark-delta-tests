ARG PYTHON_VARIANT=3.8-bookworm
FROM mcr.microsoft.com/devcontainers/python:${PYTHON_VARIANT}

#
# Install JRE
# ----------------
USER root

ARG openjdk_version="17"

RUN apt-get update --yes && \
  apt-get install --yes --no-install-recommends \
  "openjdk-${openjdk_version}-jre-headless" \
  ca-certificates-java && \
  apt-get clean && rm -rf /var/lib/apt/lists/*

USER vscode
# ----------------


# Install Spark
# ----------------
USER root

RUN set -ex; \
  apt-get update; \
  apt-get install -y gnupg2 wget bash tini libc6 libpam-modules krb5-user libnss3 procps net-tools gosu libnss-wrapper; \
  mkdir -p /opt/spark; \
  mkdir /opt/spark/python; \
  mkdir -p /opt/spark/examples; \
  mkdir -p /opt/spark/work-dir; \
  mkdir -p /home/spark; \
  chmod g+w /opt/spark/work-dir; \
  touch /opt/spark/RELEASE; \
  chown -R vscode:vscode /opt/spark; \
  chown -R vscode:vscode /home/spark; \
  echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su; \
  rm -rf /var/lib/apt/lists/*

# Install Apache Spark
# https://downloads.apache.org/spark/KEYS
ENV SPARK_TGZ_URL=https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
  SPARK_TGZ_ASC_URL=https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz.asc \
  GPG_KEY=FD3E84942E5E6106235A1D25BD356A9F8740E4FF

RUN set -ex; \
  export SPARK_TMP="$(mktemp -d)"; \
  cd $SPARK_TMP; \
  wget -nv -O spark.tgz "$SPARK_TGZ_URL"; \
  wget -nv -O spark.tgz.asc "$SPARK_TGZ_ASC_URL"; \
  export GNUPGHOME="$(mktemp -d)"; \
  gpg --batch --keyserver hkps://keys.openpgp.org --recv-key "$GPG_KEY" || \
  gpg --batch --keyserver hkps://keyserver.ubuntu.com --recv-keys "$GPG_KEY"; \
  gpg --batch --verify spark.tgz.asc spark.tgz; \
  gpgconf --kill all; \
  rm -rf "$GNUPGHOME" spark.tgz.asc; \
  \
  tar -xf spark.tgz --strip-components=1; \
  chown -R vscode:vscode .; \
  mv jars /opt/spark/; \
  mv bin /opt/spark/; \
  mv sbin /opt/spark/; \
  mv kubernetes/dockerfiles/spark/decom.sh /opt/; \
  mv examples /opt/spark/; \
  mv kubernetes/tests /opt/spark/; \
  mv data /opt/spark/; \
  mv python/pyspark /opt/spark/python/pyspark/; \
  mv python/lib /opt/spark/python/lib/; \
  mv R /opt/spark/; \
  chmod a+x /opt/decom.sh; \
  cd ..; \
  rm -rf "$SPARK_TMP";

COPY entrypoint.sh /opt/

RUN set -ex; \
  chmod a+x /opt/entrypoint.sh; \
  chown vscode:vscode /opt/entrypoint.sh;

ENV SPARK_HOME /opt/spark

# Устанавливаю мавен и качаю либы
ENV MAVEN_TGZ_URL=https://archive.apache.org/dist/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz

RUN mkdir -p /tmp/mvn
COPY pom.xml /tmp/mvn/

RUN set -ex; \
  mkdir -p /tmp/mvn; \
  cd /tmp/mvn; \
  pwd; \
  wget -nv -O maven.tgz "$MAVEN_TGZ_URL"; \
  tar -xf maven.tgz --strip-components=1; \
  chmod a+x ./bin/mvn; \
  ./bin/mvn dependency:copy-dependencies -DoutputDirectory=${SPARK_HOME}/jars; \
  cd /; \
  rm -rf /tmp/mvn;

# WORKDIR /opt/spark/work-dir

USER vscode
# ----------------

RUN pip install \
  delta-spark==3.1.0 \
  pyspark==3.5.1 \
  pandas==1.5.3 \ 
  boto3 \
  grpcio-status \
  grpcio \
  pyarrow \
  ipykernel \
  python-dotenv \
  requests \
  hvac \
  faker \
  pytest \
  pytest-benchmark

ENV PYTHONPATH "$PYTHONPATH:/workspace:/workspace/src"