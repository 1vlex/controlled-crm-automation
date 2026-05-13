FROM odoo:17.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr tesseract-ocr-rus tesseract-ocr-eng \
    poppler-utils imagemagick \
 && rm -rf /var/lib/apt/lists/*

USER odoo
