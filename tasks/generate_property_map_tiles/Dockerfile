# This Containerfile is a mix of two documentation sources:
# https://cloud.google.com/run/docs/quickstarts/jobs/build-create-shell#writing
# https://cloud.google.com/run/docs/tutorials/gcloud#code-container

# ----------

# Use a gcloud image based on debian:buster-slim for a lean production container.
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM gcr.io/google.com/cloudsdktool/cloud-sdk:slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends gdal-bin && \
    rm -rf /var/lib/apt/lists/*

# Execute next commands in the directory /workspace
WORKDIR /workspace

# Copy over the script to the /workspace directory
COPY script.sh .

# Just in case the script doesn't have the executable bit set
RUN chmod +x ./script.sh

# Run the script when starting the container
CMD [ "./script.sh" ]