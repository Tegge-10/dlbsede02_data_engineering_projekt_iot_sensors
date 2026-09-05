# Use a slim, official Python base image to keep the container small and reduce the attack surface
FROM python:3.12-slim

# Set the working directory inside the container; all subsequent paths are relative to this
WORKDIR /app

# Copy only the dependency list first (not the full source yet).
# This lets Docker cache the pip install layer, so rebuilds are fast unless requirements.txt itself changes
COPY requirements.txt .

# Install Python dependencies listed in requirements.txt (e.g. pymongo, pandas)
# --no-cache-dir keeps the final image smaller by not storing pip's download cache
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the actual application code into the image
COPY data_pipeline.py .

# Note: the sample data itself is NOT copied here.
# It is mounted at runtime via the bind mount defined in docker-compose.yml (./SampleData:/app/data),
# so the CSV data stays outside the image and can be swapped without rebuilding

# Run the loader script once when the container starts.
# The script is expected to connect to MongoDB (via the MONGO_URI environment variable),
# process the batch, log the result, and then exit
CMD ["python", "data_pipeline.py"]