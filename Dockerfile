FROM apache/airflow:2.10.5
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER root

# Install required dependencies
RUN apt-get update && apt-get install -y wget curl unzip gnupg ca-certificates libglib2.0-0 libnss3 libgconf-2-4 libfontconfig1 libxss1 libappindicator3-1 libasound2 fonts-liberation libatk-bridge2.0-0 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 libu2f-udev libvulkan1 xdg-utils

# Set Chrome version
ENV CHROME_VERSION=121.0.6167.139

# Download Chrome (for testing) and Chromedriver from official archive
RUN wget -q https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/${CHROME_VERSION}/linux64/chrome-linux64.zip && \
    wget -q https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/${CHROME_VERSION}/linux64/chromedriver-linux64.zip && \
    unzip chrome-linux64.zip && mv chrome-linux64 /opt/chrome && \
    unzip chromedriver-linux64.zip && mv chromedriver-linux64/chromedriver /usr/bin/chromedriver && \
    chmod +x /usr/bin/chromedriver && \
    rm -rf *.zip chromedriver-linux64

# Create symlink for chrome binary
RUN ln -s /opt/chrome/chrome /usr/bin/google-chrome

USER airflow
