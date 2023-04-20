FROM debian:bullseye-slim

ENV JAVA_HOME=/usr/lib/jvm/openjdk-17 \
    PYTHON=python3

RUN apt-get update && apt-get install -y \
      ca-certificates \
      curl \
      apt-transport-https \
      python3 \
    && rm -rf /var/lib/apt/lists/*

# https://adoptium.net/en-GB/installation/linux
RUN mkdir -p /etc/apt/keyrings \
    && curl --location --silent --show-error https://packages.adoptium.net/artifactory/api/gpg/key/public -o /etc/apt/keyrings/adoptium.asc \
    && echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list > /dev/null \
    && apt-get update && apt-get install -y \
      temurin-17-jdk \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/temurin-17-jdk-* $JAVA_HOME

# https://maven.apache.org/download.cgi
RUN curl --location --output /tmp/download.tar.gz --silent --show-error https://downloads.apache.org/maven/maven-3/3.9.1/binaries/apache-maven-3.9.1-bin.tar.gz \
    && cd /tmp \
    && tar -xzf download.tar.gz \
    && echo 'd3be5956712d1c2cf7a6e4c3a2db1841aa971c6097c7a67f59493a5873ccf8c8b889cf988e4e9801390a2b1ae5a0669de07673acb090a083232dbd3faf82f3e3  /tmp/download.tar.gz' | sha512sum --check --quiet \
    && mv apache-maven* /opt/apache-maven \
    && rm download.tar.gz

# Install our own CAs on the image.
# Assumes Linux Debian based image.
# JAVA_HOME needed by update-ca-certificates hook to update Java with changed system CAs.
COPY CAs/* /usr/local/share/ca-certificates/
COPY CustomCAs/* /usr/local/share/custom-ca-certificates/
RUN echo 'jdk.tls.disabledAlgorithms=jdk.tls.disabledAlgorithms=SSLv3, TLSv1, RC4, DES, MD5withRSA, DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon, NULL' > /testkit.java.security \
    && update-ca-certificates

ENV PATH=$JAVA_HOME/bin:/opt/apache-maven/bin:$PATH
