# Innowise internship Python task
## Piotr Ciupiński

### General description
The task was to create a data schema for a relational database and integrate it with Python script to load and retrieve data from said database. Additionally, the task required preparing for queries to retrieve data, proposing possible indexes for optimization and enable different formatting options (JSON, XML) for the output data.

### Repository structure
Since the program is small, I decided that separating Python files into packages was unnecessary. The only additional directory - **SQLqueries** is used to store SQL queries used during development, for analysis and index creation.

### Running the program

Running the program require Docker for the database container and Python (I used version 3.14.4 but other versions on which you can install the requirements should be fine).

After cloning or downloading the repository, inside the main directory use **docker-compose up -d** command to download postgres image and create the database container. The container is set to run and restart automatically. The database will be running on localhost, port 5432.

Install all the python requirements using **pip install -r requirements.txt** (only a couple of small packages).

Connection parameters default to the values in **docker-compose.yml**. To override them, set the `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST` and/or `POSTGRES_PORT` environment variables, or put them in a **.env** file next to the code (real environment variables take precedence). The required tables are created automatically on startup, so no manual schema setup is needed.

To run the program, simply run the **main.py** file. The displayed messages should explain what to do and what options you have - everything regarding loading, querying and selecting output format can be done through the console app. The program has the default database connection parameters pre-programmed, but if you change them in the docker-compose, you can pass them from a .env file (check **db_connector.py**).


### Database indexes

I created 2 indexes for optimizing queries. The queries creating the indexes as well as the reasoning behind my choices are present in the **index_ideas.sql** file in the **SQLqueries** directory.