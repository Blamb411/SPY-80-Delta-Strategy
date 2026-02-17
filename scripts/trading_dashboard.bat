@echo off
REM ============================================================
REM TRADING DASHBOARD - Master Control Panel
REM ============================================================
REM Combines all trading strategies and analysis tools
REM ============================================================

:menu
cls
echo.
echo  ============================================================
echo                    TRADING DASHBOARD
echo  ============================================================
echo    %date% %time%
echo  ============================================================
echo.
echo    OPTIONS STRATEGIES
echo    ------------------
echo    1. SPY 80-Delta Call Monitor (check positions/P&L)
echo    2. Put Credit Spread Scanner (find opportunities)
echo.
echo    ANALYST ESTIMATES
echo    -----------------
echo    3. Collect Daily Estimate Snapshot (LSEG)
echo    4. Live Revision Momentum Analysis
echo    5. Revision Momentum Backtest
echo.
echo    RUN ALL
echo    -------
echo    6. Morning Routine (Monitor + Scanner + Revisions)
echo.
echo    0. Exit
echo.
echo  ============================================================
echo.
set /p choice="  Select option: "

if "%choice%"=="1" goto spy_monitor
if "%choice%"=="2" goto put_scanner
if "%choice%"=="3" goto collect_estimates
if "%choice%"=="4" goto live_revisions
if "%choice%"=="5" goto backtest
if "%choice%"=="6" goto morning_routine
if "%choice%"=="0" goto end

echo Invalid choice. Press any key to try again...
pause >nul
goto menu

:spy_monitor
cls
echo.
echo ============================================================
echo SPY 80-DELTA CALL STRATEGY MONITOR
echo ============================================================
echo.
cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\Claude Options Trading Project\Strategies\80-Delta Call Strategy"
python monitor_positions.py
echo.
pause
goto menu

:put_scanner
cls
echo.
echo ============================================================
echo PUT CREDIT SPREAD SCANNER
echo ============================================================
echo NOTE: ThetaData Terminal must be running for IV Rank
echo ============================================================
echo.
cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\GitHub Repos\Put-Credit-Spreads"
python put_spread_scanner.py
echo.
pause
goto menu

:collect_estimates
cls
echo.
echo ============================================================
echo COLLECTING ANALYST ESTIMATE SNAPSHOTS
echo ============================================================
echo NOTE: LSEG Workspace must be running
echo ============================================================
echo.
cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\GitHub Repos\Valuation-and-Predictive-Factors"
python predictive/estimate_tracker.py --collect
echo.
pause
goto menu

:live_revisions
cls
echo.
echo ============================================================
echo LIVE REVISION MOMENTUM ANALYSIS
echo ============================================================
echo NOTE: LSEG Workspace must be running
echo ============================================================
echo.
cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\GitHub Repos\Valuation-and-Predictive-Factors"
python predictive/estimate_tracker.py --live --lookback 30
echo.
pause
goto menu

:backtest
cls
echo.
echo ============================================================
echo REVISION MOMENTUM BACKTEST
echo ============================================================
echo.
echo Select backtest type:
echo   1. Quick (6 months, requires LSEG)
echo   2. Full (2 years, requires LSEG)
echo   3. Yahoo-based (no LSEG needed)
echo.
set /p bt_choice="Enter choice (1/2/3): "

cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\GitHub Repos\Valuation-and-Predictive-Factors"

if "%bt_choice%"=="1" (
    python predictive/revision_backtest.py --quick
) else if "%bt_choice%"=="2" (
    python predictive/revision_backtest.py --full
) else (
    python predictive/revision_backtest.py --yahoo
)
echo.
pause
goto menu

:morning_routine
cls
echo.
echo ============================================================
echo MORNING TRADING ROUTINE
echo ============================================================
echo Running: SPY Monitor + Put Scanner + Live Revisions
echo ============================================================
echo.

echo.
echo [1/3] SPY 80-Delta Position Monitor
echo ============================================================
cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\Claude Options Trading Project\Strategies\80-Delta Call Strategy"
python monitor_positions.py

echo.
echo.
echo [2/3] Put Credit Spread Scanner
echo ============================================================
cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\GitHub Repos\Put-Credit-Spreads"
python put_spread_scanner.py

echo.
echo.
echo [3/3] Live Revision Momentum
echo ============================================================
cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\GitHub Repos\Valuation-and-Predictive-Factors"
python predictive/estimate_tracker.py --live --lookback 30

echo.
echo ============================================================
echo MORNING ROUTINE COMPLETE
echo ============================================================
pause
goto menu

:end
echo.
echo Goodbye!
exit /b 0
