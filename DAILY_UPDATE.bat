@echo off
title The Lab - V10 Automated Pipeline
color 0A

echo.
echo ========================================================
echo [1/4] Running V10 Data Pipeline (Steps 1-4)...
echo       (Scraping Games, calculating Efficiency & SOS)
echo ========================================================
python 05_run_pipeline.py

echo.
echo ========================================================
echo [2/4] Running ESPN Shot Quality Scraper...
echo ========================================================
python 1_Data_Miner.py

echo.
echo ========================================================
echo [3/4] Bundling Files for The Lab...
echo ========================================================
git add .

echo.
echo ========================================================
echo [4/4] Deploying to Cloud...
echo ========================================================
git commit -m "Daily V10 Update: %date% %time%"
git push -u origin main

echo.
echo ========================================================
echo SUCCESS! The Lab is live with fresh V10 Data.
echo ========================================================
pause