Big Data Project – Part 2 (2.1 → 2.2)
 Overview
Part 2 focuses on improving performance and extending the analytics pipeline for scalability and real-world applications.

 Project Structure
map-reduce-assignment2/
├── part2  
├── part2.2/   
Each subfolder contains:
mapper.py
reducer.py
run.sh

 Prerequisites
Python 3.x
Hadoop Streaming
Bash terminal


Dataset Requirement
Download the Data and make sure the Dataset is present inside the part2 directory with the name of clickstream_large.csv 



How to Run
Step 1: Navigate to the Directory present for map-reduce
cd /mapreduce-test
Step 2: Navigate to the required part
cd part2/part2.X
Step 2: Make script executable
chmod +x run.sh
Step 3: Make sure the path is correct.(Optional)
As run.sh file first tries to start the hadoop cluster make sure the path is correct based on the structure of the directory or change the path using vim or nano editor in the run.sh .

Step 3: Run the job
./run.sh


 Output & Logs
Outputs stored in respective folders
Logs available in logs/ for debugging

 Technologies Used
Python (MapReduce)
Hadoop Streaming
Shell scripting



