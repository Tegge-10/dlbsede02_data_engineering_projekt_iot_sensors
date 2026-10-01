# IoT Sensor Data Pipeline

## Chosen Assignment
This project addresses Task 1: "Choose a suitable database and store the data in batches". The system reads raw sensor data from a CSV file, validates and cleans it, and loads it into a MongoDB database in fault-tolerant batches. The scenario simulates a municipality that has installed various sensors throughout the city to measure environmental metrics (temperature, humidity, smoke, motion, light, etc.).

## Use Case and Dataset
### Use Case: Warning System for Citizens

The central use case of this project is a warning app for citizens. Picture Maja, a resident in a neighborhood where the city has just installed the first environmental sensors. Maja has activated the warning app on her smartphone and receives a push notification as soon as a sensor near her detects critical values for smoke, carbon monoxide, or LPG. For this use case, what matters is that the most recent measurements arrive in the database reliably, completely, and in a timely manner, so that the warning logic can rely on correct data.

Within this bigger system, the present solution acts as the ingestion and persistence layer. It does not expose an API or dashboard itself, but guarantees that every valid sensor reading reliably and idempotently ends up in a collection that downstream applications can build upon, and that every batch outcome is logged so the administrator's monitoring and retry workflow is supported in practice. The batch processing is intended to run hourly in the future. However, the interval can be adjusted as needed. This way, the warning app can display early warnings based on reliably provided data.

### Why This Dataset Is Suitable

The publicly available "Environmental Sensor Telemetry Data" dataset from Kaggle is used as the sample dataset (https://www.kaggle.com/datasets/garystafford/environmental-sensor-data-132k). It contains 405,184 measurements from three identically built sensor arrays (temperature, humidity, CO, LPG, smoke, light, motion) collected over a period of eight days (July 12–19, 2020). This short but dense collection period reflects exactly the starting situation described in the assignment: the city has only just begun installing sensors and has so far collected only the first roughly 500,000 measurements. For the warning-app use case, this is entirely sufficient, since what matters here is the timeliness and reliability of individual readings rather than observation over several years.

### Growth and Architectural Decision

The current dataset deliberately represents only the first test week of the newly installed sensors. In practice, the data volume will continue to grow over the coming months and years, both through the ongoing collection from existing sensors and through new, additional sensor types whose data structure is not yet finally defined today. This is precisely why MongoDB was chosen as a document-oriented, schema-flexible database: new sensor types can simply be added as additional fields in the future, without having to migrate existing data. Containerization with Docker further ensures that the system can be moved unchanged from a local prototype into a distributed cloud environment, where the growing measurements collection can be scaled horizontally, while smaller, stable collections such as ingestion_log and rejected_records remain unsharded. The architecture is therefore designed from the outset to accommodate growth both in the number of measurements and in the diversity of future sensor data.

## Tech Stack

| Component | Technology |
|---|---|
| Database | MongoDB 7.0 (official Docker image) |
| Processing | Python 3.12, pandas, PyMongo |
| Containerization | Docker, Docker Compose |
| Version control | Git, GitHub |

The entire pipeline runs fully containerized and has no dependencies that bind it to a specific local machine — it has been successfully tested on both Windows and macOS.

## System Requirements

To run this project locally, you need:

- **Docker Desktop** (current version, for Windows, macOS, or Linux) — [download here](https://www.docker.com/products/docker-desktop/). Docker Desktop must be running before executing the pipeline.
- **Git** — usually pre-installed on macOS (check with `git --version`), installable on Windows via [git-scm.com](https://git-scm.com/downloads).
- A terminal or command-line interface (Terminal.app on macOS, PowerShell or Git Bash on Windows).
- At least **500 MB of free disk space** for Docker images, the database volume, and the raw data.
- No local Python or MongoDB installation is required — both run exclusively inside the Docker containers.

## Step-by-Step Instructions

The following steps work identically on Windows, macOS, and Linux.

### 1. Install and start Docker Desktop

Download Docker Desktop for your operating system and processor architecture, install it, and launch the application. Wait until the Docker icon shows that Docker is running.

### 2. Install GIT

Check whether Git is already installed by running git --version in a terminal. On macOS, Git is usually pre-installed. On Windows, download and install it from git-scm.com, keeping the default options. On Linux, install it via your package manager (e.g., sudo apt install git).

### 3. Clone the repository

Open a terminal and navigate to the folder where you want to store the project, for example:

```bash
cd ~/Desktop
```

Then clone the repository:

```bash
git clone https://github.com/Tegge-10/dlbsede02_data_engineering_projekt_iot_sensors.git
```


### 4. Navigate into the project folder

```bash
cd dlbsede02_data_engineering_projekt_iot_sensors/Workdir
```

All subsequent commands are run from this folder.

### 5. Build and start the containers

```bash
docker compose up --build
```

This command automatically does the following:

1. Pulls the official MongoDB 7.0 image from Docker Hub and starts a database container.
2. Builds a custom loader image based on Python 3.12, installing all required libraries.
3. Starts the loader container, which connects to the database, reads the bundled sample CSV file, cleans it, and inserts it into MongoDB in batches of 10,000 rows.

The whole process takes roughly 2–5 minutes depending on your internet connection and machine, mostly due to downloading the Docker images.

### 6. Recognizing a successful run

The terminal will first show the build and startup process, followed by the MongoDB logs, and finally the loader's output. A successful run ends with a summary similar to the following:

```
sensor_loader | Loaded 405184 valid rows after cleaning.
sensor_loader | Batch 1/41: success (inserted=10000, duplicates_skipped=0)
...
sensor_loader | Done. 41/41 batches succeeded, 0/41 batches failed.
sensor_loader | Total new documents inserted: 405171
sensor_loader | Total duplicates skipped (already loaded previously): 13
sensor_loader exited with code 0
```

The loader container then exits automatically (`exited with code 0`), since it is designed as a one-off batch job. The MongoDB container keeps running in the background.

### 7. Verifying the loaded data and monitoring the ingestion log (operations)

To inspect the inserted data directly in the database, open a new terminal window (the containers keep running) and connect to the MongoDB shell:

```bash
docker exec -it sensor_mongodb mongosh
```

Inside the shell:

```javascript
use sensor_data
db.measurements.countDocuments()
db.measurements.findOne()
db.rejected_records.countDocuments()
db.ingestion_log.find().pretty()
```

`measurements` holds the valid, cleaned sensor readings. `rejected_records` contains rows whose timestamp or device ID could not be interpreted — stored unchanged, with a timestamp and rejection reason, instead of being discarded. `ingestion_log` contains one entry per batch attempt, each with a `status` field (`success`, `failed`, or `retry_success`) and, for failed batches, an `error` field describing the cause.

This log is also the basis for routine operational checks: after a run, confirm that the number of `status: success` entries matches the expected batch count, and that `duplicates_skipped` stays low on a first-time load (a rising number on a fresh load would indicate the loader is unexpectedly re-processing old data). If a batch shows `status: failed`, the `error` field identifies the root cause (e.g., a malformed reading, a temporary network drop, or a container restart mid-batch) so the underlying issue can be fixed before retrying.

### 8. Retrying failed batches

Because every batch attempt is logged to `ingestion_log`, failed batches can be identified and re-processed without re-running the entire pipeline. A standalone script queries this collection for batches with status `failed`, re-reads exactly the corresponding rows from the source file, and repeats the insert for these rows only:

```bash
docker compose run --rm loader python retry_failed_batches.py
```

Since every document uses the same deterministic ID as the original load (see "Notes on Idempotency" below), this retry is safe even if part of the originally failed batch had already been inserted — MongoDB skips existing documents instead of duplicating them. Each retry attempt is logged as its own entry (`retry_success` or `failed`), so the full history of a batch stays traceable instead of being overwritten.

### 9. Stopping and resetting the system

To stop all containers:

```bash
docker compose down
```

To additionally delete all stored database data and start completely from scratch next time:

```bash
docker compose down -v
```

You can restart the system at any time afterward simply with `docker compose up --build`.

## Notes on Idempotency

Each measurement document is assigned a deterministic ID built from the device ID and timestamp. Re-running the loader (e.g., after a crash or for testing purposes) therefore does not create duplicate entries — already existing records are detected and skipped as duplicates without causing the pipeline to fail. This same mechanism is what makes the retry script (see step 8) safe to run repeatedly.
