# File Structure
```
-----------------------
├── README.md
├── FP_Growth.cpp             //The cpp file containing code for FP Growth algorithm
├── book_example.txt            //Input file, from book
├── test_transaction.txt        //another input file, for testing further
├── website_example.txt         //another input file

```

# Running code
```
g++ file_name.cpp
./a.out input_data.txt

EG:
----
g++ FP_Growth.cpp
./a.out book_example.txt
```

# Changing min support
* The comment is in the code is self explanatory
* To change minimum support (20% by default), change the value of ```float min_support``` at line no:154 in the main function (line no 145)
* Input min support as % (ie, real number between 0 to 100)

# Input file
* There are three input files : book_example.txt, test_transaction.txt, website_example.txt


# Input File format (for Custom Input)
* Must be .txt format
* Input should follow following structure:
```
trsansaction_name1(string) transaction_id1(int) transaction_id2(int) ...
trsansaction_name2(string) transaction_id1(int) transaction_id2(int) ...
.
.
.

Eg
-----
T100 1 2 5
T200 2 4
T300 2 3

```