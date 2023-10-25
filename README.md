# Getting Started <br /> 

---

To get a local copy up and running follow these simple example steps.
## Prerequisites
* Python3.10
* Postgres
* Airflow
* Clickhouse

## Usage
### 1. python script
1.1. Download dataset from UCI from a [link](https://archive.ics.uci.edu/dataset/502/online+retail+ii) or in [link](https://drive.google.com/drive/folders/1s1ctAHywhgIv-GnBnPGmCEHHmJc20-ZB?usp=sharing) and put `online_retail_II.xlsx` file into a `python_script/dataset/raw` directory. 
1.2. Install the required packages from `requirements.txt`
```angular2html
pip install -r requirements.txt
```
1.3. Modify `db_info.env` file
```angular2html
PG_HOST="" 
PG_PORT=5432
PG_NAME=""
PG_USER=""
PG_PASSWORD=""
```
* PG_HOST: "localhost" if database is installed on your own local system otherwise IP address of system where PostgreSQL database is installed <br>
* PG_PORT: default port for PostgreSQL server is "5432" <br>
* PG_NAME: the database name (default is "postgres") <br>
* PG_USER: the user name (default user is "postgres") <br>
* PG_PASSWORD: the password which you gave when you installed PostgreSQL database for "postgres" user <br>

1.4. Create postgres table as a `assignment/sql_task/pg_online_retail.sql` file

1.5. Execute python script

* Open the Terminal and run `main.py` file with the --insert flag.
```angular2html
python3 main.py --insert 
```
* To enable DEBUG mode. only run the following command:
```angular2html
python3 main.py
```

* Structure <br>
![Alt text](https://github.com/vannguyende/user_img/blob/0e01a42ee6d1de7003aa069d0854577d11751054/structure.png)

1.6. View analysis notebook (optional)

Recompiling `analysis.ipynb` file

### 2. Airflow

Dependences: <br>
* airflow-clickhouse-plugin <br>
* airflow-providers-clickhouse <br>
* apache-airflow-providers-postgres

2.1. Deploy dags to airflow cluster <br>
Note: 
* Need to map `dags` directory with `dags_folder` as a config in airflow. To simplify, we can copy dag file to dags_folder dir in airflow.
The same with `plugins` directory, we need to map with `plugins_folder`.

We can verify in airflow UI <br>
![Alt text](https://github.com/vannguyende/user_img/blob/8b907984931724575385cde45c27cd4556971cc4/dags.png)

2.2. Create airflow connection <br>
2.2.1. Postgres connection <br>
![Alt text](https://github.com/vannguyende/user_img/blob/5018ae6668ced2d8133cbc274b48966d8aca8efa/postgres_db.png)

2.2.2. Clickhouse connection <br>
![Alt text](https://github.com/vannguyende/user_img/blob/5018ae6668ced2d8133cbc274b48966d8aca8efa/clickhouse_db.png)

2.3. Trigger dag <br>

We access the graph to check the health status of each task  <br>
![Alt text](https://github.com/vannguyende/user_img/blob/c515c978623dfebe291ed25971fe052709183276/grap-airflow.png) <br>


