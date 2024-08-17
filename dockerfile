FROM rust:1.76.0

RUN

ADD Cargo.lock Cargo.toml .
RUN cargo fetch


WORKDIR /usr/src/datafusion-parallelism

# sudo apt install valgrind
# sudo apt install git
# sudo apt install build-essential
# sudo apt install linux-perf
# . "$HOME/.cargo/env"
# curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# git clone https://github.com/jamesfer/datafusion-parallelism.git
