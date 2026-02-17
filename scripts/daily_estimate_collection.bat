@echo off
REM Daily Analyst Estimate Collection
REM Schedule this with Windows Task Scheduler to run at market close (4:30 PM ET)

echo ============================================================
echo Daily Analyst Estimate Collection
echo %date% %time%
echo ============================================================

cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\GitHub Repos\Valuation-and-Predictive-Factors"

REM Activate conda environment if needed
REM call conda activate trading

echo.
echo Collecting estimate snapshots from LSEG...
python predictive/estimate_tracker.py --collect

echo.
echo Running live revision analysis...
python predictive/estimate_tracker.py --live --lookback 30

echo.
echo ============================================================
echo Collection complete!
echo ============================================================
pause
