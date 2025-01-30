FROM python:3.11-slim

# Evitar prompts durante la instalación
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    postgresql-client \
    redis-tools \
    git \
    curl \
    build-essential \
    libpq-dev \
    wget \
    gnupg2 \
    lsb-release \
    && rm -rf /var/lib/apt/lists/*

# Crear usuario no root
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

# Configurar el directorio de trabajo
WORKDIR /workspace

# Copiar requirements.txt primero
COPY requirements.txt .

# Instalar solo las herramientas de desarrollo básicas
RUN pip install --no-cache-dir \
    black \
    flake8 \
    mypy \
    pytest \
    pytest-cov

# NO instalar requirements.txt aquí, lo haremos en post-create.sh

# Cambiar al usuario no root
USER $USERNAME

# Configurar alias útiles
RUN echo 'alias ll="ls -la"' >> ~/.bashrc \
    && echo 'alias python="python3"' >> ~/.bashrc \
    && echo 'alias pip="pip3"' >> ~/.bashrc