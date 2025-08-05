# Local Spark developement setup
This is setup for developement environment for Apache Spark. It's not intended to be used outside of testing, as the main data transformation is done through a cloud spark cluster through Dataproc

## Setup
0. copy&paste or soft-link the ".env" file in project root folder into spark folder
1. docker compose up -d 
2. ssh into container through tools like vscode's remote ssh + docker plugin 
