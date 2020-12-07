/*
Generate random number x. Create x transactions. For each transaction, generate a random number y
where 1<=y<=n where n is no of transactions


● Compile the program and run the binary
● This program expects command line arguments in the form: ./object_file output_file
● Any error is recorded in log.txt file as shown in the output
*/

#include <iostream>
#include <stdlib.h>

int main(int argc,char *argv[]){
    //expects command line argument output_file.txt
    int x,n,t;
    //for storing the error log
    FILE *log_ptr = fopen("log.txt","a");
    FILE *fout = NULL;

    //check if output file is given as command line argument
    if(argc!=2){
        fprintf(log_ptr,"Error: Invalid command line arguments\n");
        return -1;
    }

    fout = fopen(argv[1],"w");
    if(fout==NULL){
        fprintf(log_ptr,"Error: File %s couldn't be opened\n",argv[1]);
        return -1;
    }

    //set the random seed, change the seed to compare the answer with yours
    srand(1);
    //generate x(no of transaction randomly)
    const int MAX = 10;
    x = rand()%MAX+1;

    for(int i=1;i<=x;i++){
        fprintf(fout,"T%d ",i);
        n = rand()%MAX + 1;
        for(int j=1;j<=n;j++){
            //generate random transaction
            t = rand()%n+1;
            fprintf(fout,"%d ",t);
        }
        fprintf(fout,"\n");
    }
    return 0;   
}