@echo off
REM scripts/setup_task_scheduler.bat
REM Creates a Windows Task Scheduler job to run the pipeline daily at midnight.

SET TASK_NAME="ShipmentObservabilityPipeline"
SET PYTHON_PATH=python
SET PROJECT_DIR=%~dp0..

echo Creating Windows Scheduled Task: %TASK_NAME%...
schtasks /create /tn %TASK_NAME% /tr "\"%PYTHON_PATH%\" -m orchestration.run_pipeline" /sc daily /st 00:00 /ru "%USERNAME%" /f

echo.
echo Task %TASK_NAME% created successfully!
echo The pipeline will run daily at midnight, execute checks, export snapshots, and push to GitHub automatically.
pause
