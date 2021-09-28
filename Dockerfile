FROM python:3.9

COPY ./requirements.txt ./requirements.txt

RUN pip3 install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

COPY ./data ./data

COPY ./dashapp.py ./dashapp.py

EXPOSE 8050

CMD ["python","dashapp.py"]