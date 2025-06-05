# Cardano 

The Cardano Tasks can be broken down into 2 main parts, followed by data and proposal grant analysis done on Hex Platform for Analysis and Dashboard

## 1) Scraping of Cardano Grant Data
The Grants data are being scraped from Ideascale and Project Catalyst sites which can be found in the [Ideascale](/Users/eugeneleejunping/Documents/cardano_grants/ideascale) and [Project Catalyst](/Users/eugeneleejunping/Documents/cardano_grants/project_catalyst) folders and moved into CSV files for further analyses.

## 2) ETL Pipeline that siphons Cardano Blocks and Transactions data 
A non-realtime ETL pipeline that siphons blocks, block transactions, transactions (inclusive of UTXO) data periodically
at high throughput into S3 and Postgres, orchestrated by Airflow.

### Architecture (updated)
![image](./images/cardano_etl_pipeline_architecture.png)

The cardano grants are being broken down into 2 different types of sites:
- Ideascale: This was the site where proposals for Cardano Grant 9 were being hosted at
- Project Catalyst: This was the site where proposals from Cardano Grant 10 and beyond were being hosted at

The contents of the grants are being extracted using Selenium and Beautiful Soup.


## Project Setup

### Create venv and install dependencies

```commandline
poetry shell 
poetry install --no-root
```

### Setup postgresql@14

Project requires postgresql@14

Spin up a local instance of postgresql@14

```commandline
brew services start postgresql@14
```

Connect to the local instance, and create a new database

```commandline
psql -d postgres
CREATE DATABASE cardano;
```

Finally, use alembic to create the tables

```commandline
alembic upgrade head
```

## 

```commandline
psql -d postgres
CREATE DATABASE cardano
```

Finally, use alembic to create the tables

```commandline
alembic upgrade head
```

### Running the data pipeline to extract and load cardano block, block transactions, transactions and transaction utxo raw info to S3, transformed info to S3 and then to database

Step 1: Populate the .env file with environment variables

Step 2: Run the pipeline

```commandline
export PYTHONPATH=.
python src/etl_pipelines/cardano_blocks_to_s3_pipeline.py
python src/etl_pipelines/s3_to_db_cardano_blocks_pipeline.py
python src/etl_pipelines/cardano_block_transactions_to_s3_pipeline.py
python src/etl_pipelines/s3_to_db_cardano_block_transactions_pipeline.py
python src/etl_pipelines/cardano_transactions_to_s3_pipeline.py
python src/etl_pipelines/s3_to_db_cardano_transactions_pipeline.py
python src/etl_pipelines/cardano_tx_utxo_to_s3_pipeline.py
python src/etl_pipelines/s3_to_db_cardano_tx_utxo_pipeline.py
```

### To query the database (Postgresql)

Connect to the local instance, change database to crated database (cardano)

```commandline
psql -d postgres
\c cardano
```

```sql
 -- To get the top 10 Cardano Protocols by transaction count
SELECT reference_script_hash, COUNT(*) AS protocol_count
FROM cardano_tx_utxo_input tui 
WHERE reference_script_hash IS NOT NULL
GROUP BY reference_script_hash
ORDER BY protocol_count DESC
LIMIT 10;

 -- To get the top 10 Cardano Protocols by volume


-- How to get the top 10 Cardano Protocols by 


-- To check what have been ingested into transactions table but not tx_utxo tables
SELECT tr.block_height AS block
FROM cardano_transactions tr LEFT JOIN cardano_tx_utxo_input tui ON tr.hash=tui.hash
WHERE tui.address IS NULL
ORDER by block ASC;
```


## Connect to AWS EC2 Instance

Change directory to wherever ssh key is being downloaded to
```commandline
cd ~/.ssh
```
(run “pwd” to check working directory)

(run “ls”  to check if file is there)

Allow owner to read the file
(the pem file is named "cardano_grant.pem", edit it according to the pem file you have created)
```commandline
chmod 400 "cardano_grant.pem"
```

Connect to ec2 instance's Public IPv4 DNS
(edit "ec2-54-252..." accordingly)
```commandline
ssh -i ssh -i cardano_grant.pem ec2-user@ec2-54-252-123-110.ap-southeast-1.compute.amazonaws.com
```

