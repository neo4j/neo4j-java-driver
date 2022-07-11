FROM maven:3.8.6-openjdk-18-slim

RUN apt-get --quiet --quiet update \
    && apt-get --quiet --quiet install -y bash python3 \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHON=python3
ENV PATH=$JAVA_HOME/bin:$PATH

# Install our own CAs on the image.
# Assumes Linux Debian based image.
# JAVA_HOME needed by update-ca-certificates hook to update Java with changed system CAs.
COPY CAs/* /usr/local/share/ca-certificates/
COPY CustomCAs/* /usr/local/share/custom-ca-certificates/
RUN echo 'jdk.tls.disabledAlgorithms=jdk.tls.disabledAlgorithms=SSLv3, TLSv1, RC4, DES, MD5withRSA, DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon, NULL' > /testkit.java.security \
    && update-ca-certificates
