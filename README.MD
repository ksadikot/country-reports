Program will run with command "python main.py"

The program is an interactive program that will initialize all required tables and then ask users for input for report generation.
Users will be asked to either generate report A or B where users can enter the input: "a" || "A" || "b" || "B" or quit to exit

The structure of my python modules is I have created required functions for each required command such as:
 - create table
 - delete table
 - load records(bulk load)
 - add individual record
 - delete individual record
 - dump/display table
 - query modules that generate report A and B 

To use modules you must use "import dynamoModules" and call individuals functions sending the dynamo client with each call. 
To make changes to the database tables you can add or delete items. To add you can use either add_econ_item or add_non_econ_item where you have to pass an object that will be stored as an item onto the table
To delete you can use delete_item where you have to specifiy the table as well as the country

I have changed all the files to have Country Name as the header.
I have also stored the missing information as a json to be paresed and uploaded to the required items