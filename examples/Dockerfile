# Starting from a Python 3.9 base image
FROM malevichai/app:python11_v0.1

# Transfer the requirements.txt file
COPY requirements.txt requirements.txt

# Process the installation of dependencies
RUN if test -e requirements.txt; then pip install --no-cache-dir -r requirements.txt; fi

# Populate the image with our application's code
COPY ./apps ./apps