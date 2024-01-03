FROM python:3.11 as base

COPY ./malevich /malevich

RUN pip install /malevich
RUN pip install -r /malevich/requirements.dev.txt

CMD [ "pytest",  "/malevich/tests/" ]
