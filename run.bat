@echo off

echo Starting the script

rem Set the full path to your scripts directory (update this to your actual path)
set SCRIPT_DIR=C:\Users\Nothing\Documents\SEMISTER 7\DISTRIBUTED SYSTEMS\PROJECT\Project

rem Print the script directory for debugging
echo Script Directory: %SCRIPT_DIR%
echo.

rem Delete the ./photo_store folder
rd /s /q "%SCRIPT_DIR%\photo_store"

rem Start directory services in new tabs
for /L %%p in (5001,1,5005) do (
    echo Starting directory service on port %%p
    wt -w 0 nt --title "Directory Service %%p" -- cmd /k python3 "%SCRIPT_DIR%\haystack_dir.py" --port %%p 
)


rem Start cache servers in new tabs
for /L %%p in (6001,1,6003) do (
    echo Starting cache server on port %%p
    wt -w 0 nt --title "Cache Server %%p" -- cmd /k python3 "%SCRIPT_DIR%\haystack_cache.py" --port %%p
)

rem Start the main web server in a new tab
echo Starting main web server on port 8000
wt -w 0 nt --title "Web Server" -- cmd /k python3 "%SCRIPT_DIR%\haystack_webserver.py"

echo All services started
pause
