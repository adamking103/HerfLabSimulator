@echo off
title The Lab - V10 Automated Pipeline
color 0A

echo.
echo ==============================================================================
echo [1/5] Running V10 Data Pipeline (Steps 1-4)...
echo       (Scraping Games, calculating Efficiency & SOS)
echo ==============================================================================
python 05_run_pipeline.py

echo.
echo ==============================================================================
echo [2/5] Running ESPN Shot Quality Scraper...
echo ==============================================================================
python 1_Data_Miner.py

echo.
echo ==============================================================================
echo [3/5] Generating V10 Betting Sheet (Bible Simulator)...
echo ==============================================================================
:: "auto" tells the script to skip the menu and run all games immediately
python Bible_Simulator_V10_EXPERIMENTAL.py auto

echo.
echo ==============================================================================
echo [4/5] Bundling Files for The Lab...
echo ==============================================================================
git add .

echo.
echo ==============================================================================
echo [5/5] Deploying to Cloud...
echo ==============================================================================
git commit -m "Daily V10 Update: %date% %time%"
git push -u origin main

echo.
echo ==============================================================================
echo SUCCESS! The Lab is live with fresh V10 Data & Betting Sheets.
echo ==============================================================================
pause