@echo off
echo Starting 3-node Raft cluster in separate windows...

start "raftkv-node1" cmd /k "java -cp target/classes;target/dependency/* com.raftkv.App node1 9001 node2:localhost:9002 node3:localhost:9003"
timeout /t 1 /nobreak >nul

start "raftkv-node2" cmd /k "java -cp target/classes;target/dependency/* com.raftkv.App node2 9002 node1:localhost:9001 node3:localhost:9003"
timeout /t 1 /nobreak >nul

start "raftkv-node3" cmd /k "java -cp target/classes;target/dependency/* com.raftkv.App node3 9003 node1:localhost:9001 node2:localhost:9002"

echo.
echo All 3 nodes launching in separate windows.
echo Wait a couple seconds for leader election to complete.
echo.