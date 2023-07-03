///////////    Running the Prototype ///////////////

1. Go to the DataLoading folder where you will find the files to load data into your database
             a.  Change the database information in the  'config.txt' file according to your database
             b.  Run 'create-tables.py'
             c.  Run 'constraintsOfDimensions.py'
             d.  Run 'constraintsOfFacts.py'
             e.  Run 'load-dimensions.py'
             f.   Run 'LoadClimateFactTable.py' , 'LoadCopernicusFactTable.py' , 'LoadModisAquaFactTable.py', 'LoadPathFinderFactTable.py'  
N.B:   Make sure that your python interpreter has netCDF4, psycopg2, OSQP libraries intalled

2. Go to the Eris-main folder where you will find the filse to run prototype

           a. Change the database information in the  'config.txt' file according to your database
           b. Double click 'run.bat' file  and the prototype screen will be shown
           c. Select the options that you want to try and click the LOAD button and wait until loading is completed
           d. In tab 2, you will get the options to select a ground truth but if you see there is only one option under one table, do not select any, leave it as it is otherwise you can check and update       the schema
           e. In tab 3, you can give the quries as input or as choose the query file from your directory and click the RUN button
                   ... Make Sure you copy the spec file
           f. You will able to see the results in a new window

3. Some query example is given in the Queries folder
