@echo off
REM Revision Momentum Backtest
REM Tests whether analyst estimate revisions predict returns

echo ============================================================
echo Revision Momentum Backtest
echo %date% %time%
echo ============================================================

cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\GitHub Repos\Valuation-and-Predictive-Factors"

REM Activate conda environment if needed
REM call conda activate trading

echo.
echo NOTE: Requires LSEG Workspace to be running
echo.

echo Select backtest type:
echo   1. Quick (6 months)
echo   2. Full (2 years)
echo   3. Yahoo-based (no LSEG needed)
echo.
set /p choice="Enter choice (1/2/3): "

if "%choice%"=="1" (
    python predictive/revision_backtest.py --quick
) else if "%choice%"=="2" (
    python predictive/revision_backtest.py --full
) else if "%choice%"=="3" (
    python predictive/revision_backtest.py --yahoo
) else (
    echo Invalid choice. Running Yahoo-based backtest...
    python predictive/revision_backtest.py --yahoo
)

echo.
echo ============================================================
pause
