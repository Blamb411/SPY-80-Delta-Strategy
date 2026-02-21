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
echo    3. PCS Auto-Trader [IBKR Paper]
echo.
echo    ANALYST ESTIMATES
echo    -----------------
echo    4. Collect Daily Estimate Snapshot (LSEG)
echo    5. Live Revision Momentum Analysis
echo    6. Revision Momentum Backtest
echo.
echo    RUN ALL
echo    -------
echo    7. Morning Routine (Monitor + PCS + Scanner + Revisions)
echo.
echo    0. Exit
echo.
echo  ============================================================
echo.
set /p choice="  Select option: "

if "%choice%"=="1" goto spy_monitor
if "%choice%"=="2" goto put_scanner
if "%choice%"=="3" goto pcs_autotrader
if "%choice%"=="4" goto collect_estimates
if "%choice%"=="5" goto live_revisions
if "%choice%"=="6" goto backtest
if "%choice%"=="7" goto morning_routine
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
cd /d "C:\Users\Admin\Put-Credit-Spreads"
python put_spread_scanner.py
echo.
pause
goto menu

:pcs_autotrader
cls
echo.
echo ============================================================
echo PCS AUTO-TRADER [IBKR Paper]
echo ============================================================
echo NOTE: TWS must be running with paper trading on port 7497
echo ============================================================
echo.
echo   1. Full Cycle (monitor + scan SPY)
echo   2. Monitor Only (check open positions)
echo   3. Scheduler (continuous auto-trading)
echo   4. Dry Run (check signals, no orders)
echo   5. Status (show open positions)
echo   6. Performance Dashboard
echo   7. Trade History
echo   0. Back to main menu
echo.
set /p pcs_choice="  Select option: "

cd /d "C:\Users\Admin\Put-Credit-Spreads"

if "%pcs_choice%"=="1" (
    python ibkr_put_spread.py --paper
) else if "%pcs_choice%"=="2" (
    python ibkr_put_spread.py --monitor-only --paper
) else if "%pcs_choice%"=="3" (
    echo.
    echo Starting scheduler mode (Ctrl+C to stop)...
    python ibkr_put_spread.py --scheduler --paper --ticker SPY,QQQ
) else if "%pcs_choice%"=="4" (
    python ibkr_put_spread.py --dry-run --paper
) else if "%pcs_choice%"=="5" (
    python ibkr_put_spread.py --status
) else if "%pcs_choice%"=="6" (
    python ibkr_put_spread.py --performance --paper
) else if "%pcs_choice%"=="7" (
    python ibkr_put_spread.py --history
) else if "%pcs_choice%"=="0" (
    goto menu
)
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
echo Running: SPY Monitor + PCS Monitor + Put Scanner + Revisions
echo ============================================================
echo.

echo.
echo [1/4] SPY 80-Delta Position Monitor
echo ============================================================
cd /d "C:\Users\Admin\OneDrive\Desktop\Investment Trading Programs\Claude Options Trading Project\Strategies\80-Delta Call Strategy"
python monitor_positions.py

echo.
echo.
echo [2/4] PCS Position Monitor [IBKR Paper]
echo ============================================================
cd /d "C:\Users\Admin\Put-Credit-Spreads"
python ibkr_put_spread.py --monitor-only --paper

echo.
echo.
echo [3/4] Put Credit Spread Scanner
echo ============================================================
cd /d "C:\Users\Admin\Put-Credit-Spreads"
python put_spread_scanner.py

echo.
echo.
echo [4/4] Live Revision Momentum
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
