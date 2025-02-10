#!/bin/bash
set -e

echo "🌊 Starting Splash..."

# Make sure we are in the correct directory
cd /workspace

# Configure environment variables
echo "🔧 Configuring environment variables..."
export PYTHONPATH=/workspace
export DISPLAY=:99
export QT_QPA_PLATFORM=offscreen
export PATH="/home/vscode/.local/bin:$PATH"

# Verify installation
echo "✅ Verifying installation..."
if ! python3 -c "import PyQt5.QtWebKit" 2>/dev/null; then
    echo "❌ Error: PyQt5.QtWebKit is not installed correctly"
    echo "🔄 Attempting to install system dependencies..."
    sudo apt-get update
    sudo apt-get install -y python3-pyqt5 python3-pyqt5.qtwebkit python3-pyqt5.sip
fi

# Create necessary directories
echo "📁 Creating directories..."
sudo mkdir -p /tmp/.X11-unix
sudo chmod 1777 /tmp/.X11-unix
mkdir -p /workspace/logs/splash
sudo chown -R vscode:vscode /workspace/logs

# Clean up X11 temporary files
echo "🧹 Cleaning up temporary files..."
sudo rm -f /tmp/.X*-lock

# Start Xvfb
echo "🖥️ Starting Xvfb..."
Xvfb :99 -screen 0 1024x768x16 &
sleep 2

# Verify that Xvfb is working
if ! xdpyinfo -display :99 >/dev/null 2>&1; then
    echo "❌ Error: Xvfb did not start correctly"
    exit 1
fi

echo "✨ Splash configuration completed!" 