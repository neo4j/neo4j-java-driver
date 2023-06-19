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

COPY --from=maven:3.9.2-eclipse-temurin-17 /usr/share/maven /opt/apache-maven

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
