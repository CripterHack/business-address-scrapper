FROM python:3.11-bullseye

# Evitar interacciones durante la instalación de paquetes
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV DISPLAY=:99
ENV QT_QPA_PLATFORM=offscreen
ENV PYTHONPATH=/workspace:/usr/lib/python3/dist-packages
ENV ENABLE_AI_FEATURES=false

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    echo "deb http://deb.debian.org/debian bullseye contrib non-free" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y \
    build-essential \
    curl \
    git \
    libpq-dev \
    postgresql-client \
    python3-dev \
    python3-pip \
    python3-venv \
    redis-tools \
    sudo \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-spa \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    libmagic1 \
    xvfb \
    x11-utils \
    python3-pyqt5 \
    python3-pyqt5.qtwebkit \
    python3-pyqt5.sip \
    python3-sip \
    qttools5-dev-tools \
    qt5-qmake \
    libqt5webkit5-dev \
    libqt5webkit5 \
    libxrandr2 \
    libxss1 \
    libxtst6 \
    libnss3 \
    libnspr4 \
    libasound2 \
    libgbm1 \
    xauth \
    python3-xvfbwrapper \
    python3-progress && \
    rm -rf /var/lib/apt/lists/*

# Crear usuario vscode
RUN useradd -m -s /bin/bash -u 1000 vscode \
    && echo "vscode ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && mkdir -p /home/vscode/.vscode-server/extensions \
    && chown -R vscode:vscode /home/vscode

# Crear directorios X11 con permisos correctos
RUN mkdir -p /tmp/.X11-unix && \
    chmod 1777 /tmp/.X11-unix

# Establecer el directorio de trabajo
WORKDIR /workspace

# Copiar scripts de inicialización
COPY .devcontainer/init-splash.sh /usr/local/bin/
COPY .devcontainer/start-splash.sh /usr/local/bin/
COPY .devcontainer/post-create.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/init-splash.sh \
    /usr/local/bin/start-splash.sh \
    /usr/local/bin/post-create.sh

# Script condicional para instalar dependencias de IA
COPY .devcontainer/install-ai-deps.sh /tmp/
RUN chmod +x /tmp/install-ai-deps.sh && \
    /tmp/install-ai-deps.sh

# Copiar requirements.txt si existe
COPY requirements.txt* /workspace/

# Configurar Python y permisos
RUN update-alternatives --install /usr/bin/python python /usr/local/bin/python3 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3 1 && \
    mkdir -p /usr/local/lib/python3.11/dist-packages && \
    chown -R root:staff /usr/local/lib/python3.11 && \
    chmod -R g+w /usr/local/lib/python3.11

# Instalar dependencias base de Python
RUN apt-get update && \
    apt-get install -y \
    python3-dev \
    python3-setuptools \
    python3-wheel \
    python3-packaging \
    python3-six \
    python3-pkg-resources \
    python3-xvfbwrapper \
    python3-pip \
    python3-progress \
    python3-retrying \
    python3-appdirs && \
    apt-get remove -y python3-pip && \
    wget -O /tmp/get-pip.py https://bootstrap.pypa.io/pip/3.6/get-pip.py && \
    python3 /tmp/get-pip.py --no-warn-script-location && \
    rm /tmp/get-pip.py && \
    python3 -m pip install --no-cache-dir splash==3.5.0 scrapy-splash --no-deps && \
    if [ -f "requirements.txt" ]; then \
        python3 -m pip install --no-cache-dir -r requirements.txt; \
    fi && \
    rm -rf /var/lib/apt/lists/* && \
    chown -R vscode:vscode /usr/local/lib/python*/dist-packages

# Crear cache directories
RUN mkdir -p /home/vscode/.cache/scraper \
    && chmod 755 /home/vscode/.cache/scraper

# Set environment variables for cache
ENV CACHE_DIR=/home/vscode/.cache/scraper \
    PYTHONPATH=/workspace

# Cambiar al usuario vscode
USER vscode

# Mantener el contenedor en ejecución
CMD ["sleep", "infinity"]

# Configurar alias útiles y PATH
RUN echo 'export PATH="/home/vscode/.local/bin:$PATH"' >> ~/.bashrc \
    && echo 'export PYTHONPATH=/workspace:/usr/lib/python3/dist-packages:$PYTHONPATH' >> ~/.bashrc \
    && echo 'alias ll="ls -la"' >> ~/.bashrc \
    && echo 'alias python="python3"' >> ~/.bashrc \
    && echo 'alias pip="pip3"' >> ~/.bashrc

# El comando de inicio se maneja en docker-compose.yml