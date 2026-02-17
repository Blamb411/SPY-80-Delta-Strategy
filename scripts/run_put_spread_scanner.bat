@echo off
REM Put Credit Spread Scanner
REM Scans for high IV rank put spread opportunities

echo ============================================================
echo Put Credit Spread Scanner
echo %date% %time%
echo ============================================================

cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\GitHub Repos\Put-Credit-Spreads"

REM Activate conda environment if needed
REM call conda activate trading

echo.
echo NOTE: Requires ThetaData Terminal to be running for IV Rank calculation
echo.

python put_spread_scanner.py

echo.
echo ============================================================
pause
