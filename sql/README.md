[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=12540940&assignment_repo_type=AssignmentRepo)
# csci422-sql-assgnmt

This assignment will exercise your ability to create a star schema analytical data model using SQL.  As with other assignments, the resources used for the assignment will be cloud hosted.  This assignment will involve creating the Azure resources using a PowerShell script instead of manually in the Azure portal.

## Azure setup
In the AzureSetup subfolder, there is a PowerShell script and a configuration file.  You will need to edit the configuration file to map to your subscription and have unique names for the Azure resources.  Please watch the video posted to Blackboard for more background on the setup process.

## Database setup
In the CreateTransactionalDB subfolder, there is a SQL script that will create the source database with data.  For reference, there is a picture of the schema in the same folder.  Please watch the video posted to Blackboard for more background on the database setup process.

## Create the analytical data model
We are acting as data engineers for the business analyst team at Northwinds.  The analyst want us to enable insights on order information, specifically the order details or lines.  

The analysts desire to query the order details based on:
- Order details - price, quantity, discount, date
- Product - name, quantity per unit, unit price
- Product supplier - company name, region, country, active date
- Product category - category name
- Customer - company name, region, and country
- Order shipper - Company name
- Order employee - name, title, country

Your goal is to create a star schema (note - not snowflake) with a single fact table.  The fact table name should be prefixed with FACT_.  The dimension tables should be prefixed with DIM_.  Proper data types should be used for all columns.  The only required fields are the ones that support the analyst requirements listed above.

The analyst wants to do some analysis based on the product supplier as they plan to change suppliers for some products in the future.  To support this, the supplier dimension should support slowly changing dimensions (SCD) using the Type 2 pattern.

Once the schema is created, you will need to write the SQL queries to do the initial loading of the data into the star schema.  The queries should be wrapped in a stored procedure so that it can be executed from an orchestration engine.

## What to hand in

### Azure setup - 15 points
After executing the PowerShell script, take a screenshot of the resulting resource group.  Put the screenshot in the Evidence subfolder.

### Star schema creation - 30 points
To simplify the exercise, we will create the star schema in the same database as the transactional data.  To delineate the analytics table, create a new SQL schema using the name 'ss' (abbreviation for 'star schema'). 

In the Evidence folder, place a diagram of your star schema using the tool of your choice (could even be hand written and scanned).  The schema should contain table names, a list of columns in each table, and the relationships between the tables.

In the SQLTransformation folder, place your DDL (Data Definition Language) code to create the star schema in a single SQL file.

In the Evidence folder, place a screenshot of the tree view of your query editor after creating the star schema.

### Star schema data load - 30 points
The star schema that you created will need to be loaded with data from the transactional database.  Write the DML (Data Manipulation Language) code required to do this initial load in a stored procedure.  The stored procedure SQL file should be located in the SQLTransformation folder.

Once the data is loaded, export the data from each table you have created and store the exported data in the Evidence folder (one CSV per table).

Remember to commit all changes to the repo and push the code to 'main'.  There's no need to submit anything to the Blackboard assignment.

If needed, updates or clarifications will be posted on the discussion forum after the assignment is created.