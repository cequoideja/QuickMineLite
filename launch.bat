@echo off
title QuickMine Lite
cd /d "%~dp0"
echo ========================================
echo   QuickMine Lite - Process Mining
echo ========================================
echo.

:: Verifier si Python est disponible
py --version >nul 2>&1
if errorlevel 1 (
    python --version >nul 2>&1
    if errorlevel 1 (
        echo ERREUR: Python non trouve.
        echo Installez Python depuis https://python.org
        pause
        exit /b 1
    )
    set PYTHON=python
) else (
    set PYTHON=py
)

:: Installer les dependances si streamlit n'est pas present
%PYTHON% -m streamlit --version >nul 2>&1
if errorlevel 1 (
    echo Installation des dependances...
    echo.
    %PYTHON% -m pip install -r requirements.txt
    echo.
    if errorlevel 1 (
        echo ERREUR: L'installation a echoue.
        pause
        exit /b 1
    )
    echo Installation terminee.
    echo.
)

echo Lancement de l'application...
echo.
%PYTHON% -m streamlit run app.py
pause
