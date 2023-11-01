FROM debian:bullseye-slim

ENV JAVA_HOME=/usr/lib/jvm/openjdk-17 \
    PYTHON=python3

RUN apt-get update && apt-get install -y \
      ca-certificates \
      curl \
      apt-transport-https \
      python3 \
      p11-kit \
    && rm -rf /var/lib/apt/lists/*

# https://hub.docker.com/_/eclipse-temurin
COPY --from=eclipse-temurin:17-jdk /opt/java/openjdk $JAVA_HOME

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
RUN echo 'jdk.tls.disabledAlgorithms=jdk.tls.disabledAlgorithms=SSLv3, TLSv1, RC4, DES, MD5withRSA, DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon, NULL' > /testkit.java.security

# https://github.com/adoptium/containers/issues/293
# https://github.com/adoptium/containers/pull/392
# https://github.com/adoptium/containers/blob/f6d4923380ecb1ec4b0d58c633ebb0aeed4c8332/17/jdk/ubuntu/jammy/entrypoint.sh#L23
RUN update-ca-certificates \
    && trust extract --overwrite --format=java-cacerts --filter=ca-anchors --purpose=server-auth "$JAVA_HOME/lib/security/cacerts"

ENV PATH=$JAVA_HOME/bin:/opt/apache-maven/bin:$PATH
