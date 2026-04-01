Big Data Project – Part 1 (1.1 → 1.5)
 Overview
Part 1 focuses on building a complete data processing pipeline using the MapReduce paradigm on the NYC Parking Violations dataset. It covers data cleaning, transformation, and exploratory analytics to extract meaningful insights.

Project Structure
map-reduce-assignment2
├──  part1/  
├── part1.2/   
├── part1.3/   
├── part1.4/   
├── part1.5/   

Each subfolder contains:
mapper.py
reducer.py
run.sh

 Prerequisites
Make sure the following are installed:
Python 3.x
Hadoop Streaming (or local MapReduce simulation)
Bash (Linux/Mac terminal)


Dataset Requirement
Download the Data and make sure the Dataset is present inside the part1 directory with the name of parking_raw.csv.




How to Run
Step 1: Navigate to the Directory present for map-reduce
cd /mapreduce-test
Step 2: Navigate to the required part
cd part1/part1.X
Step 2: Make script executable
chmod +x run.sh
Step 3: Make sure the path is correct.(Optional)
As run.sh file first tries to start the hadoop cluster make sure the path is correct based on the structure of the directory or change the path using vim or nano editor in the run.sh .

Step 3: Run the job
./run.sh


 Tasks Breakdown
 Part 1.1 – Data Cleaning & Validation
Removes invalid or incomplete records
Handles missing values
Standardizes formats
 Output: Clean dataset ready for analysis

 Part 1.2 – Time-Based Analysis
Extracts time features (hour/day)
Aggregates violations by time
 Output:
Peak hours for violations
Daily trends

 Part 1.3 – Violation Type Analysis
Counts frequency of violation types
Sorts violations by occurrence
 Output:
Most common parking violations

 Part 1.4 – Location-Based Analysis
Aggregates violations by borough/street/zip
Identifies hotspots
 Output:
High-violation areas
Geographic distribution

 Part 1.5 – Multi-Dimensional Analysis
Combines multiple features (e.g., time + location)
Performs grouped aggregations
 Output:
Advanced insights across dimensions

 Output & Logs
Output files are generated in each task directory
Logs are stored in logs/
Use logs for debugging and verification
   Technologies Used
Python (MapReduce – Mapper & Reducer)
Hadoop Streaming
Shell Scripting

