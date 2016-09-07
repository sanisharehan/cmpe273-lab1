FROM python:3.4.0
MAINTAINER Your Name "sanisha27@gmail.com"
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
CMD ["app.py"]
