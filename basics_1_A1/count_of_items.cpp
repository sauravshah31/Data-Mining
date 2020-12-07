/*
Find the support/count of items

● Compile the program and run the binary
● This program expects command line arguments in the form: ./object_file input_file1
input_file2 ... output_file
● Any error is recorded in log.txt file as shown in the output
*/

#include <iostream>
#include <vector>
#include <unordered_map>

using namespace std;

void read_file(FILE *fin,unordered_map<int,int> &count){
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

        //skip transaction name
        while(i<n && transaction[i]!=' '){
            i++;
        }
        //skip the space
        i++;

        //get current transaction amounts
        while(i<n){
            if(transaction[i]==' '){
                count[curr]++;
                curr=0;
            }else{
                curr = curr*10 + (transaction[i]-'0');
            }
            i++;
        }
        count[curr]++;
        curr=0;
        transaction.clear();
    }
}

void write_to_file(FILE *fout,unordered_map<int,int> &count){
    for(const pair<int,int> &m:count){
        fprintf(fout,"%d %d\n",m.first,m.second);
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
    unordered_map<int,int> count;

    vector<FILE*> files_ptr;
    files_ptr.reserve(no_of_files);

    for(int i=0;i<no_of_files;i++){
        FILE *fin = fopen(argv[i+1],"r");
        if(fin==NULL){
            fprintf(log_ptr,"Error: Failed to open file :%s :count_of_items.cpp\n",argv[i]);
            continue;
        }
        
        read_file(fin,count);
        fclose(fin);
    }

    
    FILE *fout =  fopen(argv[argc-1],"w");
    if(fout==NULL){
        fprintf(log_ptr,"Error: Failed to open file :%s :count_of_items.cpp",argv[argc-1]);
    }else{
        write_to_file(fout,count);
        fclose(fout);
    } 
    
    return 0;
}