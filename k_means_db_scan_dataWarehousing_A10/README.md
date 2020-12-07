# Runnin the script
* Q1(k_means_clustering.py) and Q2(db_scan_algo.py) depends on ```numpy```
* Q3(data_warehousing.py) depends on ```sqlite3```

### Steps
* Install the dependencies (numpy,sqlite3)
```
create virtual environment (python v>=3.8) [OPTIONAL]
activate the virtualenvironment

pip install -r requirements.txt

python file_name.py
```

# INPUT
* Q1,Q2 input is as per question
* Q3, a dummy database is made

# Running q1,q2
```
python k_means_clustering.py
python db_scan_algo.py
```
* Output is displayed in the terminal


# Running q3
```
python data_warehousing.py
```
* A dummy database(structure of which is commented out in the script itself)
* output is displayed in the terminal
* A sample output has been echoed to "datacube_output.txt"
