# Install Maven 3.6, Java 11, Java 8 and Python3
FROM maven:3.6.3-openjdk-8

RUN apt-get --quiet --quiet update \
    && apt-get --quiet --quiet install -y bash python3 \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHON=python3
ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH=$JAVA_HOME/bin:$PATH

# Install our own CAs on the image.
# Assumes Linux Debian based image.
# JAVA_HOME needed by update-ca-certificates hook to update Java with changed system CAs.
COPY CAs/* /usr/local/share/ca-certificates/
RUN update-ca-certificates
