# Apriori Algorithm
## Running the code
* Compile the code
* Run the binary and pass the input file name as command line argument (must by .txt, multiple filenames is accepted)
* test_transaction.txt is default input for both the questions (Please don't open these files using any fancy text editors, c++ expects ASCII encoding but most syatems use 'utf-8' by default).
* Testing apriori_without_prune_step
```
g++ apriori_without_prune_step.cpp -o apriori_without_prune_step
./apriori_without_prune_step test_transaction.txt

g++ apriori_without_prune_step.cpp -o apriori_without_prune_step
./apriori_without_prune_step test_transaction.txt file2.txt ...
```
* Testing apriori_with_prune_step
```
g++ apriori_with_prune_step.cpp -o apriori_with_prune_step
./apriori_with_prune_step test_transaction.txt

g++ apriori_with_prune_step.cpp -o apriori_with_prune_step
./apriori_with_prune_step test_transaction.txt file2.txt ...
```

## Changin the min_support & min_confidence
* Change the  **kwargs** in the **main()** function (towards the end)
* kwargs["min_confidence"] = float -> used to specify minimum confidence in percentage
```
kwargs["min_confidence"] = 50; //min confidence is 50%
```
* kwargs["min_support"] = float -> used to specify minimum support in percentage
```
kwargs["min_support"] = 60; //min support is 60%
```

## Input/Output format
* test_transaction.txt is input file for both the questions

## Input/Output format
* The input files must be .txt and the transaction must be numbers:
    ```
    T1 1 2 3
    T2 1 3
    T3 1 2
    ```
* The encoding must be **ASCII** ,not **utf-8** (most sytem uses this by default). Please use plain text editor (vi, Notepad, Notepad++, gedit, pluma)

* Ouput displays the generated frequent itemset, followed  by generated association rule. The last line contains time of execution. (apriori_without_prune_step takes longes time as no purne state) 

## Abstract Datarypes Used

* struct Series
    * Each transaction can be viewed as a series, with transaction name and transactions. Transaction name is string and the  transactions are integer.
    ```
    T1 1 2 3
    ```
    * 
* struct Dataframe
    * The entire transaction can be viewed as array of Series, called Dataframe
    ```
    T1 1 2 3
    T2 1 3
    T3 1 2
    ```
* struct Filehandler
    * this is used for  handling files


## Passing in random transaction
* The code accepts any transaction, given that the file has .txt extension, and has transaction in given format:
```
transaction_name1(string) transaction1(int) transaction2(int) ...
transaction_name2(string) transaction1(int) transaction2(int) ...
...
...
```
```
Saurav Shah
Roll No: 187157
```