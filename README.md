 ## Running the Prototype

**1.** Go to the DataLoading folder where you will find the files to load data into your database
   - Please Download and Paste the Datasets into the *Data* Folder from here: `https://drive.google.com/drive/folders/1R6opY3qY9k1ZMxYXmhf4yUywoLsxbuDM?usp=sharing`
   - Change the database information in the  'config.txt' file according to your database
   - Run 'create-tables.py'
   - Run 'constraintsOfDimensions.py'
   - Run 'constraintsOfFacts.py'
   - Run 'load-dimensions.py'
   - Run 'LoadClimateFactTable.py', 'LoadCopernicusFactTable.py', 'LoadModisAquaFactTable.py', 'LoadPathFinderFactTable.py'  

**N.B:** Make sure that your python interpreter has netCDF4, psycopg2, OSQP libraries intalled

**2.** Go to the Eris-main folder where you will find the files to run the prototype

   - Change the database information in the  'config.txt' file according to your database
   - Double click 'run.bat' file  and the prototype screen will be shown
   - Select the options that you want to try and click the LOAD button and wait until loading is completed
   - In tab 2, you will get the option to select a ground truth but if you see there is only one option under one table, do not select any, leave it as it is otherwise you can check and update the schema
   - In tab 3, you can give the queries as input or choose the query file from your directory and click the RUN button. Make Sure you copy the spec file of the queries in the Eris-main folder
   - You will able to see the results in a new window

**3.** Some query example is given in the Queries folder
