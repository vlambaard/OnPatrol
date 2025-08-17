#!/bin/bash

# macOS Virtual Environment Setup for OnPatrol
# Requires: Python 3.7+ and pip

echo "Checking for Python 3..."
if ! command -v python3 &> /dev/null; then
    echo "Python 3 not found. Please install Python 3.7+ first."
    echo "You can install it via Homebrew: brew install python"
    exit 1
fi

# Check if we're on macOS and suggest dependencies
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Detected macOS. Checking for required dependencies..."
    
    # Check for Homebrew
    if ! command -v brew &> /dev/null; then
        echo "Warning: Homebrew not found. Some dependencies may be missing."
        echo "Install Homebrew from: https://brew.sh"
    else
        echo "Installing/updating macOS dependencies via Homebrew..."
        brew install cmake pkg-config
        # For OpenCV dependencies
        brew install jpeg libpng libtiff openexr
        brew install tbb
    fi
fi

echo "Creating Python virtual environment..."
python3 -m venv .venv

echo "Activating virtual environment..."
source .venv/bin/activate

echo "Upgrading pip and setuptools..."
python -m pip install --upgrade pip
python -m pip install --upgrade setuptools wheel

echo "Installing Python dependencies..."
python -m pip install -r dependencies/requirements.txt

echo "Installing custom sqlite3worker dependency..."
cd dependencies/sqlite3worker/
python setup.py install
cd ../..

echo ""
echo "✅ Virtual environment setup complete!"
echo ""
echo "To activate the environment in the future, run:"
echo "    source .venv/bin/activate"
echo ""
echo "To deactivate when done, run:"
echo "    deactivate"