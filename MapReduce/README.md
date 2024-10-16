# MapReduce Task: Count "Attribute, Case, and Vote Lines" in `anonymous-msweb.data`

## Task Description

Write a MapReduce job that will calculate the number of "Attribute, Case, and Vote Lines" for the dataset `anonymous-msweb.data`. 

The dataset can be found in the `Data Folder`. The dataset description is also available in the same folder.

[Download the dataset here.](https://archive.ics.uci.edu/dataset/4/anonymous+microsoft+web+data)

## Solution Steps

Follow the steps below to set up the Hadoop environment with Docker, upload the dataset, and execute the MapReduce job.

### Step 1: Navigate to the Project Directory

```bash
cd "C:\Users\Artem\IdeaProjects\HadoopWithDocker\docker-hadoop"
```

### Step 2: Start the Docker Containers

Start the Hadoop cluster using Docker Compose:

```bash
docker-compose up -d
```

### Step 3: Create Directory for Files in the Namenode Container

Run the following command to create a directory for storing data and the MapReduce job inside the Namenode container:

```bash
docker exec namenode mkdir -p /home/hadoop
```

### Step 4: Copy the Dataset to the Namenode Container

Copy the dataset file anonymous-msweb.data from your local machine to the Namenode container:

```bash
docker cp "D:\datasets\anonymous+microsoft+web+data\anonymous-msweb.data" namenode:/home/hadoop/
```

### Step 5: Copy the MapReduce Job JAR File

Copy the MapReduce job JAR file MapReduce-1.0-SNAPSHOT.jar to the Namenode container:

```bash
docker cp "D:\13\uni-projects\parallel-computing\MapReduce\target\MapReduce-1.0-SNAPSHOT.jar" namenode:/home/hadoop/
```

### Step 6: Access the Namenode Container

Log into the Namenode container to run the following HDFS commands:
```bash
docker exec -it namenode /bin/bash
```

### Step 7: Leave Safe Mode

Before interacting with HDFS, leave safe mode (HDFS may automatically enter safe mode on startup):

```bash
hdfs dfsadmin -safemode leave
```

### Step 8: Clean the HDFS Input Directory (if it exists)

Delete the existing input directory in HDFS (if any) to avoid conflicts:

```bash
hdfs dfs -rm -r /input
```


### Step 9: Create a New Input Directory in HDFS

Create a new directory in HDFS to store the dataset:
```bash
hdfs dfs -mkdir /input
```


### Step 10: Upload the Dataset to HDFS

Upload the dataset to the newly created input directory in HDFS:
```bash
hdfs dfs -put /home/hadoop/anonymous-msweb.data /input/
```

### Step 11: Clean the HDFS Output Directory (if it exists)

Remove any existing output directory in HDFS to avoid conflicts with the results:
```bash
hdfs dfs -rm -r /output/result
```


### Step 12: Run the MapReduce Job

Run the MapReduce job using the uploaded dataset:
```bash
yarn jar /home/hadoop/MapReduce-1.0-SNAPSHOT.jar ua.karazin.parallelcomputing.CountLines /input/anonymous-msweb.data /output/result
```

### Step 13: View the Results

Once the job completes, view the results by reading the output file in HDFS:
```bash
hdfs dfs -cat /output/result/part-r-00000
```