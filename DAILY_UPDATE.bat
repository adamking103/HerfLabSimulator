@echo off
title The Bible V10.1 - Daily Cloud Deploy
color 0A

echo.
echo ========================================================
echo   THE BIBLE V10.1 - DAILY UPDATE PROTOCOL
echo ========================================================
echo.

:: 1. Run the Python Data Refresh
echo [1/2] Updating Data & Models...
python DAILY_REFRESH.py

:: 2. Push to GitHub (Triggers Streamlit Cloud Update)
echo.
echo [2/2] Pushing to Streamlit Cloud...
git add .
git commit -m "Daily V10.1 Update: %date%"
git push origin main

echo.
echo ========================================================
echo   SUCCESS! App is updating on mobile.
echo ========================================================
pause