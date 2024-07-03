FROM bitnami/spark:latest

USER root

# Install Python and required packages
RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install pyspark==3.1.2

# Set environment variables
ENV SPARK_HOME /opt/bitnami/spark
ENV PATH $SPARK_HOME/bin:$PATH

# Copy application code
COPY . /app
WORKDIR /app

# Set the entry point for the container
ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-avro_2.12:3.1.2", "/app/stream_processor.py"]
