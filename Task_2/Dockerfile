# Use an official Python runtime as the base image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy your requirements (if any) and install them
RUN pip install boto3 pandas

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python script into the container
COPY . .

# Expose any ports if needed (for web servers); otherwise skip this step.

# Set the default command to execute your script
# CMD ["python", "task_1.py"]

# Default entrypoint (will override per task)
ENTRYPOINT ["python"]
CMD ["task_2.py"]
# CMD ["task_1.py", "task_2.py"]
