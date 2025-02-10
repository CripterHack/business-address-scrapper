#!/bin/bash
set -e

if [ "$ENABLE_AI_FEATURES" = "true" ]; then
    echo "🤖 Installing AI dependencies..."
    python3 -m pip install --user \
        PyQt5==5.15.11 \
        PyQt5-sip==12.17.0 \
        PyQt5-Qt5==5.15.16 \
        PyQtWebKit==5.15.6 \
        llama-cpp-python==0.2.56 \
        transformers==4.38.1
    echo "✨ AI dependencies installed successfully"
else
    echo "ℹ️ AI features are disabled, skipping dependency installation"
fi 