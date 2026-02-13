pip install grpcio grpcio-tools protobuf six

@echo off
setlocal enabledelayedexpansion

echo ===================================================
echo Distributed Chat System Auto IP Startup Script
echo ===================================================

:: Create data directory
if not exist data mkdir data

:: Get local machine IP address
echo Getting local machine IP address...
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /R /C:"IPv4 Address"') do (
    set ip=%%a
    set ip=!ip:~1!
    goto :foundip
)
:foundip

:: If IP not found, use localhost
if "%ip%"=="" (
    echo Unable to get IP address, using localhost instead.
    set ip=localhost
)

echo Using IP address: %ip%

:: Create data directory
if not exist "data" mkdir "data"

:: Start DNS service
echo 1. Starting DNS service (port 50051)
start cmd /k python dns_service.py --port 50051 --host 0.0.0.0

:: Wait for DNS service to start
timeout /t 3 /nobreak

:: Start first server
echo 2. Starting Server 1 (port 50050)
start cmd /k python server.py --id server1 --port 50050 --host 0.0.0.0 --dns %ip%:50051

:: Wait for first server to start
timeout /t 3 /nobreak

:: Start second server
echo 3. Starting Server 2 (port 50060)
start cmd /k python server.py --id server2 --port 50060 --host 0.0.0.0 --dns %ip%:50051 --peers server1=%ip%:50050

:: Wait for second server to start
timeout /t 3 /nobreak

:: Start third server
echo 4. Starting Server 3 (port 50070)
start cmd /k python server.py --id server3 --port 50070 --host 0.0.0.0 --dns %ip%:50051 --peers server1=%ip%:50050,server2=%ip%:50060

timeout /t 3 /nobreak

:: Display system information
echo ===================================================
echo Distributed chat system is up!
echo DNS Service: %ip%:50051
echo Server1: %ip%:50050
echo Server2: %ip%:50060
echo Server3: %ip%:50070
echo ===================================================

echo.
echo To shut down the system, close all command windows.
echo.

:: Launching the GUI client
echo 5. Launching the GUI client...
echo Note: The client will try to connect to server3, if it's not the leader, it will automatically redirect.

:: Ask the user whether to start the client
set /p start_client=Start the GUI client? (Y/N):

if /i "%start_client%"=="Y" (
    python client_gui.py --server %ip%:50070 --dns %ip%:50051
) else (
    echo The client is not started. You can start it manually later:.
    echo python client_gui.py --server %ip%:50070 --dns %ip%:50051
)



endlocal