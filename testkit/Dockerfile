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

COPY --from=maven:3.9.2-eclipse-temurin-17 /usr/share/maven /opt/apache-maven

# Install our own CAs on the image.
# Assumes Linux Debian based image.
# JAVA_HOME needed by update-ca-certificates hook to update Java with changed system CAs.
COPY CAs/* /usr/local/share/ca-certificates/
COPY CustomCAs/* /usr/local/share/custom-ca-certificates/
RUN echo 'jdk.tls.disabledAlgorithms=jdk.tls.disabledAlgorithms=SSLv3, TLSv1, RC4, DES, MD5withRSA, DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon, NULL' > /testkit.java.security \
    && update-ca-certificates

ENV PATH=$JAVA_HOME/bin:/opt/apache-maven/bin:$PATH
