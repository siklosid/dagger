FROM python:3.7.4-slim-stretch

# Add your envrionment variables
#ARG ACCESS_KEY_ID
#ARG SECRET_ACCESS_KEY
#
#ENV ACCESS_KEY_ID=$ACCESS_KEY_ID
#ENV SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY


COPY reqs/base.txt .
RUN pip install -r base.txt

COPY . .
RUN python setup.py install && pip install -e .

CMD sh

