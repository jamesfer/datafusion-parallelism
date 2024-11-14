sudo apt update -y 
sudo apt install git -y
sudo apt install build-essential -y

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env"

git clone https://github.com/jamesfer/datafusion-parallelism.git
cd datafusion-parallelism/

cargo bench --bench my_benchmark
