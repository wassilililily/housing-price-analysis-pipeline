FROM apache/airflow:2.10.5

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER root

RUN apt-get update && apt-get install -y libgomp1

# Install Chromium & dependencies (multi-arch safe)
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    wget \
    curl \
    unzip \
    gnupg \
    ca-certificates \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    libxss1 \
    libappindicator3-1 \
    libasound2 \
    fonts-liberation \
    libatk-bridge2.0-0 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libu2f-udev \
    libvulkan1 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Optional: symlink for compatibility
RUN ln -s /usr/bin/chromium /usr/bin/google-chrome

USER airflow
