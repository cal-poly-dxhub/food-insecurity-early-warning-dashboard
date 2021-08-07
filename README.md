# Early Warning for Early Action Prototype dashboard 
#### Built for the World Bank with the purpose to better monitor global food insecurity
#### Completed with Cal Poly's DxHub under the challenge: "Early Warning for Early Action (Evaluating Food Security Risk)"
#### Initial data fetching and codebase provided by student team: David Ngo, Charlie Taylor,  Nathaniel Cinnamon, and Amy Ozee
#### Full-scale code refactor and AWS automation completed by Braden Michelson


## Files:
- main.py: Fetches data from sources, formats it into a consistent schema, and then uploads it to a database
- Data Injestion Record Sheet*.xlsx: Records of the indicators, their respective sources, and details to why they are or aren't in the dashboard
- Dash_AWS.pbix: Dashboard to be opened by Power BI Desktop. Requires a database connection
- Dash_Local.pbix: Dashboard to be opened by Power BI Desktop. Contains cached data without a database connection. Provided for a reference in case dashboard formatting in Dash_AWS.pbix is lost


## Requirements:
- Python 3.8 (previous versions can likely be used, just weren’t tested)
- AWS Account
- Power BI Account
- A computer with Power BI Desktop
- A computer willing to host an ongoing data gateway


## Main Steps For Reproduction:
1. Clone repository into an computer with python 3.8 and Power BI Desktop
2. Create Microsoft SQL Server in AWS RDS
3. Run data fetching and importing script with new database parameters
4. Importing data into local dashboard
5. Publish dashboard to power BI server with on-premise data gateway

## Reproduction

#### Clone repository into an computer with python 3.8 and Power BI Desktop:
1. Open up the terminal and navigate to your target directory for the new project
2. Clone the repo: “git clone https://github.com/cal-poly-dxhub/food-insecurity-early-warning-dashboard.git”
3. Navigate into the repository folder and ensure all files we cloned


#### Create Microsoft SQL Server in AWS RDS:
(other databases can be used if they are compatible with pyodbc, compatible with Power BI, and use microsoft SQL insert and create table syntax. In this case the DB_DRIVER argument and Power BI “Data source type” would need to change)
1. Sign into AWS Console with AWS account
2. Navigate to Amazon RDS service page
3. Click "Databases"
4. Click "Create Database"
5. Create a database with the options:
   - Choose a database creation method: Standard create
   - Engine options: Microsoft SQL Server
   - Edition: SQL Server Express Edition (other editions can be used, but this data currently will not exceed
   - Version: SQL Server 2019 15.00.4073.23.v1 was used, but the most recent will probably work
   - DB instance identifier: Unique name to reference later ("db instance name")
   - Master username: Username to log into database ("db username")
   - Master password: Password to log into database ("db password")
   - DB instance class: Burstable classes -> db.t3.small (not a large cpu is needed for this task at the moment)
   - Storage type: General Purpose (SSD)
   - Allocated storage: 20 (or the minimum)
   - Enable storage autoscaling: Not necessary at current stage but users choice, FAO and World Bank data are under 1 GiB
   - VPC: Your Project's VPC (See note below)
   - Subnet group: VPC's subnet (See note below)
   - Public access: Yes
   - VPC security group: Choose existing (See note below)
   - Existing VPC security groups: Choose existing (See note below)
   - Availability Zone: No preference
   - Database port: keep as default (1433) unless reasons otherwise ("db port")
   - Enable Microsoft SQL Server Windows authentication: No (unchecked)
   - In the larger Additional Configuration tab, automated backups are enabled by default to 7 days. If the data fetching script is run on a schedule it is suggested that you match that schedule here as to have backups of the last run in case the sources or endpoints change and cause errors
   - NOTE: When it comes to access permissions within your chosen security group and the VPC there are two options:
     - Allow public access through the internet (not the most secure option) or
	 - Allow the ip addresses of the computer using Power BI Desktop and the computer hosting the data gateway
6. Click "Create Database" and then click on the database instance name
7. Go to database's Connectivity & security tab
8. Refresh the page until the database is created then note the endpoint ("db endpoint") and port (“db port”)


#### Run data fetching and importing script with new database parameters:
1. Within the cloned repository’s folder, open main.py in a text editor
2. Alter the database constants to match the values given to you by the RDS
	DB_DRIVER = "SQL Server" (if using Microsoft SQL Server)
	DB_PORT = db port
	DB_SERVER = db endpoint
	DB_DATABASE = "Dashboard"
	DB_USER = db username
	DB_PASSWORD = db password
3. Save main.py
4. Within the terminal pointing at  the cloned repository’s folder run the command: “python main.py”
5. An output of “uploads complete” means all the data is now in the database. If an exception is raised, it is likely due to altered endpoints or data structure from the sources. 
    If the endpoints have changed: The hardcoded endpoints can be used to download the CSV files. If the downloads do not work, the endpoint likely does not work. Steps are provided in main.py for each source in order to reproduce the api call 
    If data structure has changed: this would require investigation into the downloaded CSVs and hard-coded column names will need to be changed in main.py. 


#### Importing data into local dashboard:
1. Open up Power BI Desktop
2. In Power BI, open Dash_AWS.pbix
	(NOTE: Dash_Local is available to be opened by power BI too. Here, the data is cached from a previous version as a reference if the database schema is altered and importing from the database is not working correctly)
3. Wait until the “Cannot load model” or a similar screen comes up and close it as this is expected.
4. In the Home tab, select “Transform data”
5. The in query editor, select “Data source settings”
6. Select the now unavailable database source and select “Change Source…”
7. Replace the server field with the new database endpoint
8. Ensure the Database field is “Dashboard” or the value of DB_DATABASE in main.py
9. Select “OK”
10. Select the new connection and then select “Edit Permissions…”
11. Under “Credentials”,  Select “Edit…”
12. Ensure you are in the “Database” tab in the left bar
13. Input the db username and db password into the fields
14. Select “Save”
15. Select “OK” in the Edit Permissions window
16. Select “Close” in the Data source settings window
17. In the top left of the home tab in the query editor, select “Close & Apply”
18. Data should now be imported and all dashboard pages should be visible and populated. 


#### Publish dashboard to power BI server with on-premise data gateway:
1. Navigate to powerbi.com and sign in with your Power BI account
2. Ensure you have a workspace that you would like to upload the dashboard to. (By default you have “Your Workspace”, but this might not be the ideal location.)
3. In Power BI, in the home tab, select “Publish”
4. Select your target workspace and select “Select”
5. On the Power BI website, navigate to your workspace
6. Select the uploaded dashboard as Type “Report”
7. The pages should be numbered, but the data should not appear. This is because a data gateway needs to be configured.
8. Navigate to “Download” in the top Power BI tab, it may be in the “...” menu.
9. Select “Data Gateway”
10. Download the gateway software installer. Decide on standard mode or personal mode based on your use case. This does not have to take place on the same instance where the Power BI Desktop app was used because a license is not needed, but a Power BI account is. The gateway should be on an instance that is available (running and internet accessible) while access to the web dashboard is needed. This is because the data is provided through a DirectQuery connection, so the data is fetched directly from the database whenever the report is accessed and this is what the gateway offers.
11. Open up the installer and go through the installation process
12. Once installation is successful, sign in with your Power BI account
13. Select “Register a new gateway on this computer”
14. Fill in your preferred data gateway name and recovery key
15. Select “Configure”
16. Ensure through the new window’s status tab that the gateway is “online and ready to be used”
17. Return to powerbi.com and navigate to “Settings” in the top Power BI tab, it may be in the “...” menu.
18. Select “Manage gateways”
19. To the right of your gateway cluster name, select “...”, and then select “ADD DATA SOURCE”
20. Name the data source your preferred name
21. In the “Data Source Type” dropdown select SQL Server. Fill in the fields just as they were inputted into Power BI Desktop with Basic Authentication Method
22. Wait for the “Connection Successful” response
23. Return to the dashboard in your workspace. Visuals should now be shown and data should be populated. The dashboard now exists with the proper connections in the Power BI web server
24. To publish the report to the web for users not within the workspace: select the dropdown “File” bar, then select your preferred method in the “Embed report” tab.
