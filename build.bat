@echo off
cmd /k "cd /d %~dp0\..\venv_win\Scripts & activate.bat & cd /d %~dp0\src & pyinstaller --noconfirm -i "cam_server.ico" --add-data="cam_server.ico;./" --add-data="deepstack_test_image.jpg;./" --add-data="ffmpeg.exe;./" --distpath ../dist --workpath ../build OnPatrolServer.py"

set /p DUMMY=Hit ENTER to continue...