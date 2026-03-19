FROM python:3.11-slim

WORKDIR /app
RUN pip install streamlit plotly pandas psycopg2-binary
COPY app.py .

EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
