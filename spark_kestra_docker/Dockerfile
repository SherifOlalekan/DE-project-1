FROM python:3.9.1

# Set the working directory
WORKDIR /app

# Install required Python libraries
RUN pip install --no-cache-dir pandas pyarrow

# Ensure Python is installed
RUN python --version

# Copy project files
COPY . /app

# Default command to start a Python shell
CMD ["python"]
