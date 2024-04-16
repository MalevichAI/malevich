# This file is an auto-generated Dockerfile for the 
# Malevich App image. You may edit this file to add
# additional dependencies to your app or set more
# specific enironment.

# Keep in mind, that source code containing
# Malevich-specific code (declaration of processors, inits, etc.)
# should be placed into ./apps directory
FROM malevichai/app:python_v0.1

# Copying requirements.txt and installing dependencies
COPY requirements.txt requirements.txt
RUN if test -e requirements.txt; then pip install --no-cache-dir -r requirements.txt; fi

# Placing source code into the image
# DO NOT change this line
COPY ./apps ./apps