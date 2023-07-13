## Installation Requirements

**1.** Install Scala version 2.13.10 [(https://www.scala-lang.org/download/2.13.10.html)]  
**2.** Install sbt version 1.8.3 [(https://www.scala-sbt.org/download.html)]  
**3.** Install python version 3.9.16 or more  
**4.** Install PostgreSQL 14 or more  
**5.** Install pgAdmin 4 or more  
**6.** Install JVM 11
 
 ## Running the Prototype

**1.** Go to the DataLoading folder where you will find the files to load data into your database
   - Please Download and Paste the Datasets into the *Data* Folder from here: `https://drive.google.com/drive/folders/1R6opY3qY9k1ZMxYXmhf4yUywoLsxbuDM?usp=sharing`
   - Change the database information in the  'config.txt' file according to your database
   - Create a Python environment using the command:    `conda create --name myenv python = version`
   - Install all the required packages using the command: `pip install -r requirements.txt`
   - Double click *run.bat* file and wait for data to be loaded into the database. Change the python environment name before running


**2.** Go to the Eris-main folder where you will find the files to run the prototype

   - Change the database information in the  'config.txt' file according to your database
   - In *python.bat* file, update the python directory according to the python.exe file in your computer
   - The auxiliary SQL functions must be created by executing the scripts in the folder SQLFunctions
   - Double click *run.bat* file  and the prototype screen will be shown. Before running this file, edit the Python interpreter name that you are using
   - Select the options that you want to try, click the LOAD button and wait until loading is completed
   - In tab 2, you will get the option to select a ground truth but if you see there is only one option under one table, do not select any, leave it as it is otherwise you can select and update the schema
   - In tab 3, you can give the queries as input or choose the query file from your directory and click the RUN button. Make Sure you copy the spec file of the queries in the Eris-main folder
   - You will able to see the results in a new window

**3.** Some query example is given in the Queries folder
