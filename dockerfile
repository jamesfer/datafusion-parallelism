FROM rust:1.76.0

RUN apt update && apt install linux-perf -y

WORKDIR /usr/src/datafusion-parallelism
