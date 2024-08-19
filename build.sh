#!/usr/bin/bash

source ./VENV/bin/activate
cd ./src/
pyinstaller --onefile --noconfirm --clean --distpath ../dist --workpath ../build -i ./cam_server.ico --add-data=./cam_server.ico:./ --add-data=./deepstack_test_image.jpg:./ ./OnPatrolServer.py
cd ..

