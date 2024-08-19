#!/bin/bash

sudo apt update
sudo apt install -y screen python3-pip python3-venv python3-h5py tk-dev libncurses5-dev libncursesw5-dev libreadline6-dev libdb5.3-dev libgdbm-dev libsqlite3-dev libssl-dev libbz2-dev libexpat1-dev liblzma-dev zlib1g-dev libffi-dev cmake build-essential libatlas-base-dev 

echo "Creating Python VENV"
python -m venv ./VENV
echo "Switching to Python VENV"
source ./VENV/bin/activate
echo "Installing dependencies..."
python -m pip install --no-input -U pip
python -m pip install --no-input -U setuptools
python -m pip install --no-input -U wheel
python -m pip install --no-input -r ./dependencies/requirements.txt

echo "Installing custom dependencies..."
cd ./dependencies/sqlite3worker/
python setup.py install