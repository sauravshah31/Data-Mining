/*
Extract item-transaction list

● Compile the program and run the binary
● This program expects command line arguments in the form: ./object_file input_file1 input_file2 ...
output_file
● Any error is recorded in log.txt file as shown in the output
*/

#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <string>

using namespace std;

void read_file(FILE *fin,unordered_map<int,unordered_set<string>> &transaction_map){
    int curr = 0;
    vector<char> transaction;
    char c;
    while(true){
        while(true){
            c = fgetc(fin);
            if(c==EOF||c=='\n'||c=='\0')
                break;
            transaction.push_back(c);
        }
        if(feof(fin))
            break;

        int i=0;
        int n = transaction.size();
        //strip the white space
        while(n && (transaction[n-1]==' ')){
            n--;
        }

        //get transaction name
        string curr_transaction = "";
        while(i<n && transaction[i]!=' '){
            curr_transaction += transaction[i];
            i++;
        }
        //skip the space
        i++;

        //get current transaction amounts
        while(i<n){
            if(transaction[i]==' '){
                transaction_map[curr].insert(curr_transaction);
                curr=0;
            }else{
                curr = curr*10 + (transaction[i]-'0');
            }
            i++;
        }
        transaction_map[curr].insert(curr_transaction);
        curr=0;
        transaction.clear();
    }
}

void write_to_file(FILE *fout,unordered_map<int,unordered_set<string>> &transaction_map){
    for(const pair<int,unordered_set<string>> &m:transaction_map){
        //fprintf(fout,"%d %d\n",m.first,m.second);
        //write the transaction name
        fprintf(fout,"%d ",m.first);
        //write the transactions to the file
        for(const string &t:m.second){
            fprintf(fout,"%s ",t.c_str());
        }
        //go to next line
        fprintf(fout,"\n");
    }
}

int main(int argc,char *argv[]){
/*
    command line arguments: input_file1 input_file2 input_file3 ... output_file
*/
    FILE * log_ptr = fopen("log.txt","a");

    if(argc < 3){
        fprintf(log_ptr,"Error: Invalid command line arguments\n");
        return -1;
    }

    int no_of_files=argc-2;
    unordered_map<int,unordered_set<string>> transaction_map;

    vector<FILE*> files_ptr;
    files_ptr.reserve(no_of_files);

    for(int i=0;i<no_of_files;i++){
        FILE *fin = fopen(argv[i+1],"r");
        if(fin==NULL){
            fprintf(log_ptr,"Error: Failed to open file :%s :count_of_items.cpp\n",argv[i]);
            continue;
        }
        
        read_file(fin,transaction_map);
        fclose(fin);
    }

    
    FILE *fout =  fopen(argv[argc-1],"w");
    if(fout==NULL){
        fprintf(log_ptr,"Error: Failed to open file :%s :count_of_items.cpp",argv[argc-1]);
    }else{
        write_to_file(fout,transaction_map);
        fclose(fout);
    } 
    
    return 0;
}