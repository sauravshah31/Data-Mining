/*
Read Transactions from file using file operation

g++ read_database_transaction.cpp
./a.out
*/

#include <iostream>

using namespace std;

int main(){
    char file_name[100];
    int no_of_files;
    char c;

    cout<<"No of files : ";
    cin>>no_of_files;

    while(no_of_files--){
        cout<<"File name : ";
        cin>>file_name;

        FILE *fin = fopen(file_name,"r");

        if(fin==NULL){
            cout<<"Failed to load file";
            continue;
        }

        while(true){
            c = fgetc(fin);
            if(c==EOF)
                break;
            cout<<c;
        }

    }
}