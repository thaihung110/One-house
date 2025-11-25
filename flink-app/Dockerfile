FROM apache/flink:1.18.1-scala_2.12-java11

WORKDIR /opt/flink

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Install Flink S3 Plugin for MinIO
RUN echo "Add Flink S3 Plugin" && \
    mkdir -p ./plugins/s3-fs-hadoop && \
    cp ./opt/flink-s3-fs-hadoop-1.18.1.jar ./plugins/s3-fs-hadoop/

# Install Flink Kafka connector
RUN echo "-> Install JARs: Flink's Kafka connector" && \
    mkdir -p ./lib/kafka && \
    curl -L https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar \
    -o ./lib/kafka/flink-sql-connector-kafka-3.0.1-1.18.jar

# Install Iceberg Flink Runtime
RUN echo "-> Install JARs: Iceberg Flink Runtime" && \
    mkdir -p ./lib/iceberg && \
    curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.2/iceberg-flink-runtime-1.18-1.5.2.jar \
    -o ./lib/iceberg/iceberg-flink-runtime-1.18-1.5.2.jar

# Install Iceberg AWS Bundle (includes S3FileIO + all AWS SDK dependencies in one JAR)
RUN echo "-> Install JARs: Iceberg AWS Bundle (All-in-One)" && \
    curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar \
    -o ./lib/iceberg/iceberg-aws-bundle-1.5.2.jar

# Install HTTP client for OAuth2
RUN echo "-> Install JARs: HTTP Client for OAuth2" && \
    mkdir -p ./lib/http && \
    curl -L https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/4.5.14/httpclient-4.5.14.jar \
    -o ./lib/http/httpclient-4.5.14.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/httpcomponents/httpcore/4.4.16/httpcore-4.4.16.jar \
    -o ./lib/http/httpcore-4.4.16.jar

# Install Flink Shaded Hadoop (includes all Hadoop dependencies)
RUN echo "-> Install JARs: Flink Shaded Hadoop" && \
    mkdir -p ./lib/hadoop && \
    curl -L https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
    -o ./lib/hadoop/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# Copy Python requirements
COPY requirements.txt /opt/flink/requirements.txt

# Install Python dependencies
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt

# Copy Python job files
COPY jobs/ /opt/flink/jobs/

# Copy Flink configuration
COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml

# Set PyFlink environment
ENV PYFLINK_CLIENT_EXECUTABLE=python3
ENV PYFLINK_PYTHON=python3

# Entrypoint will be set in docker-compose

