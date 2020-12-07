# File Structure
```
Assignment5
------------
Q1) Improve Apriori using hash based approach
-> hashing_improvement.cpp 

Q2) Inmprove Apriori using Dynamic Itemset Counting
-> dynamic_itemset_counting.cpp

Q3) Implement Partition Based Apriori Algorithm
-> partition_based_ariori.cpp


Assignment6
------------
Q1) FP Tree Creation
-> FP_Tree.cpp
```

# Running code
```
g++ file_name.cpp
./a.out input_data.txt

EG:
----
g++ dynamic_itemset_counting.cpp
./a.out book_example.txt
```

# Input file
* There are two input files : test_transaction.txt, book_example.txt
* All the code gives same output (for 30%),ie same frequent itemset, so this is the input for all the code


# Input File format
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

