@echo off
echo This will permanently delete all node data (WAL + snapshots).
set /p confirm="Are you sure? (y/n): "
if /i "%confirm%"=="y" (
    rmdir /s /q data
    echo Data directory cleared.
) else (
    echo Cancelled.
)