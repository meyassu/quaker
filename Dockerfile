FROM python:3.9-slim

WORKDIR /usr/src/app

# Copy directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Switch to non-sudo user to run container
RUN useradd --create-home tuser
USER tuser

# Run app.py when the container launches
CMD ["python", "./src/main.py"]
