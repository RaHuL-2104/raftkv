@echo off
echo ============================================
echo  Building raftkv...
echo ============================================

call mvn clean compile -q
if %ERRORLEVEL% NEQ 0 (
    echo BUILD FAILED. See errors above.
    exit /b 1
)

echo Copying dependencies...
call mvn dependency:copy-dependencies -DoutputDirectory=target/dependency -q
if %ERRORLEVEL% NEQ 0 (
    echo DEPENDENCY COPY FAILED.
    exit /b 1
)

echo ============================================
echo  Build complete. Run start-cluster.bat next.
echo ============================================