Owner: Tanmay Sandeep Kelkar

This project can be accessed and built on your local system by cloning the repository with the command: "git clone https://github.com/tkelkar1177/WordCount.git".

The project can be built by running: "sbt clean compile run" and the test cases can be run by running: "sbt clean compile test".


The following steps detail the key points of this project:

1. The custom jar that we want to deploy on EMR is built with the command "sbt assembly".
2. The LogFileGenerator project is run to obtain our input text file with the logs that we want to run our mapreduce on.
3. We create a bucket in S3 within which we create our input folder.
4. We upload our custom jar and log file to our input folder in S3
5. We spin up a cluster passing our jar name, jar path, input and required output folder paths. We also provide a security key which was obtained while creating an EC2 instance.      We also specify the instance type that we want along with the number of master and core nodes.
6. We spin up our cluster, which runs our mapreduce program and persists the output files in the output paths that we had specified.


A video detailing the deployment steps has been uploaded to YouTube and can be viewed here: https://youtu.be/EUvUzSRQ12s
