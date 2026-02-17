@echo off
REM SPY 80-Delta Call Strategy Monitor
REM Run this to check current positions and P/L

echo ============================================================
echo SPY 80-Delta Call Strategy Monitor
echo %date% %time%
echo ============================================================

cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\Claude Options Trading Project\Strategies\80-Delta Call Strategy"

REM Activate conda environment if needed
REM call conda activate trading

echo.
python monitor_positions.py

echo.
echo ============================================================
pause
