#!/bin/bash
set -e

echo "🌊 Starting Splash..."

# Ensure X11 permissions
if [ ! -d "/tmp/.X11-unix" ]; then
    echo "📁 Creating X11 directory..."
    sudo mkdir -p /tmp/.X11-unix
    sudo chmod 1777 /tmp/.X11-unix
fi

# Clean up existing lock files
sudo rm -f /tmp/.X*-lock /tmp/.X11-unix/X*

# Check if Xvfb is already running
if pgrep -x "Xvfb" > /dev/null; then
    echo "ℹ️ Xvfb is already running"
else
    echo "🖥️ Starting Xvfb..."
    Xvfb :99 -screen 0 1024x768x16 &
    sleep 2
fi

# Verify that Xvfb is working
if ! xdpyinfo -display :99 >/dev/null 2>&1; then
    echo "❌ Error: Xvfb is not working correctly"
    exit 1
fi

# Set environment variables
export DISPLAY=:99
export QT_QPA_PLATFORM=offscreen
export PYTHONPATH=/workspace:/usr/lib/python3/dist-packages
export PATH="/home/vscode/.local/bin:${PATH}"

# Print Python paths for debugging
echo "🔍 Python path information:"
python3 -c "import sys; print('\n'.join(sys.path))"

# Verify QtWebKit installation
echo "🔍 Checking PyQt5 installation..."
if ! python3 -c "import PyQt5" 2>/dev/null; then
    echo "❌ Error: PyQt5 is not installed correctly"
fi

echo "🔍 Checking QtWebKit installation..."
if ! python3 -c "from PyQt5 import QtWebKit" 2>/dev/null; then
    echo "❌ Error: PyQt5.QtWebKit is not installed correctly"
    echo "🔄 Attempting to install system dependencies..."
    sudo apt-get update
    sudo apt-get install -y \
        python3-pyqt5 \
        python3-pyqt5.qtwebkit \
        python3-pyqt5.sip \
        python3-sip \
        qttools5-dev-tools \
        qt5-qmake \
        libqt5webkit5-dev \
        libqt5webkit5
    
    # Verificar de nuevo la instalación
    if ! python3 -c "from PyQt5 import QtWebKit" 2>/dev/null; then
        echo "❌ Error: Could not install PyQt5.QtWebKit"
        echo "🔍 Debugging information:"
        dpkg -l | grep -i pyqt
        dpkg -l | grep -i webkit
        ls -l /usr/lib/python3/dist-packages/PyQt5
        python3 -c "import PyQt5; print(PyQt5.__file__)"
        exit 1
    fi
fi

# Check if Splash is already running
if pgrep -f "python3 -m splash.server" > /dev/null; then
    echo "ℹ️ Splash is already running"
    exit 0
fi

# Start Splash
echo "🚀 Starting Splash server..."
mkdir -p /workspace/logs/splash
python3 -m splash.server --port=8051 --disable-lua-sandbox --max-timeout 300 > /workspace/logs/splash/splash.log 2>&1 &

# Wait for Splash to be ready
echo "⏳ Waiting for Splash to be ready..."
max_attempts=30
attempt=1
while ! curl -s http://localhost:8051/_ping > /dev/null; do
    if [ $attempt -ge $max_attempts ]; then
        echo "❌ Error: Splash did not respond after $max_attempts attempts"
        echo "📋 Last lines of Splash log:"
        tail -n 20 /workspace/logs/splash/splash.log
        exit 1
    fi
    echo "🔄 Attempt $attempt of $max_attempts..."
    sleep 2
    ((attempt++))
done

echo "✨ Splash is ready and running at http://localhost:8051" 