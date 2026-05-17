#!/bin/bash
set -e

echo "Updating apt and installing system dependencies..."
sudo apt-get update
sudo apt-get install -y protobuf-compiler just reuse pipx curl wget syncthing

echo "Installing dprint..."
if ! command -v dprint &> /dev/null; then
  cargo install dprint
else
  echo "dprint is already installed."
fi

echo "Ensuring path is set..."
export PATH="$HOME/.cargo/bin:$PATH"

echo "Setting SYNCTHING_BIN..."
export SYNCTHING_BIN="$(which syncthing)"

echo "Setup complete. You can now use 'just lint', 'just fmt' and 'just test'."
