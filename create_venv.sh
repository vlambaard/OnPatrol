#!/bin/bash

sudo apt update
sudo apt install -y screen python3-pip python3-venv python3-h5py tk-dev libncurses5-dev libncursesw5-dev libreadline6-dev libdb5.3-dev libgdbm-dev libsqlite3-dev libssl-dev libbz2-dev libexpat1-dev liblzma-dev zlib1g-dev libffi-dev cmake build-essential libatlas-base-dev 

echo "Creating Python VENV"
python3 -m venv .venv
echo "Switching to Python VENV"
source .venv/bin/activate
echo "Installing dependencies..."
python3 -m pip install --no-input -U pip
python3 -m pip install --no-input -U setuptools
python3 -m pip install --no-input -U wheel
python3 -m pip install --no-input -r dependencies/requirements.txt

echo "Installing custom dependencies..."
cd dependencies/sqlite3worker/
python3 setup.py install