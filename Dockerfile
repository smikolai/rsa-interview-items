FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update -qq && \
    apt-get install -y -qq python3 python3-pip && \
    pip3 install --no-cache-dir pyspark==4.0.2 jupyterlab && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /work
