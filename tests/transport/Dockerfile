FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        iproute2 \
        iputils-ping \
        gstreamer1.0-tools \
        gstreamer1.0-plugins-base \
        gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad \
        gstreamer1.0-plugins-ugly \
        gstreamer1.0-libav \
        gir1.2-gstreamer-1.0 \
        gir1.2-gst-plugins-base-1.0 \
        python3.12-venv \
        python3-gi \
        python3-gst-1.0 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.lock /tmp/requirements.lock
RUN python3.12 -m venv --system-site-packages /opt/rov-cv-venv \
    && /opt/rov-cv-venv/bin/python -m pip install --upgrade pip setuptools wheel \
    && /opt/rov-cv-venv/bin/python -m pip install --requirement /tmp/requirements.lock

ENV PATH="/opt/rov-cv-venv/bin:${PATH}"
WORKDIR /workspace
