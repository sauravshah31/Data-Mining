/*
Analyse the transactions using association rules

g++ file_name.cpp
./a.out input1.txt input2.txt ...

eg:
./a.out test_transaction.txt

Please refer to test_transaction.txt for input format
*/

#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <cassert>
#include <cmath>

using namespace std;
/*
Objective:
Derive a relationship among transactions so as to understand purchase behaviour of customers.
This relationship should aid in uplifting business growth
 
1) We start by analysing product that is most frequently bought
   i) This item should be given more priority, so we set some threshold value relative to the max frequency,
    if the frequency is less than this value, we don't analyse that product(don't include that product in out subset)
2) Given the products that customers have bought, how likely is it that they are gonna buy some other products?
   -> This can be mathematically modeled as
       {subset1} => {subset2}  , subset1 determines subset2, subset1,subset2 both being subset of items
   -> This means that if customer have bought items in subset1, there is certain probability that they are gonna buy items in subset2,
       if this probability is greater than some threshold value, we can strategize the business accordingly
   -> if 'N' be total no of items in the store then,
       |subset1| = X & |subset2| = Y,
       such that, 1<=X<=N-1,1<=Y<=N-1, subset1 ∩ subset2 = {} => X+Y<=N
       this statement means that there must be at least one element in both the subsets or at max N-1 elements in either of the subsets
   -> We programmatically determine subsets subset1 & subset2 for different values of X & Y, and then calculate the probability of occurrence
   -> One optimization would be to exclude items from set based on observation 1
   -> probability of occurrence = no_of_transactions_where_subset_is present / total_no_of_transactions
       here, a subset occurs in a transaction if all the items of subset1 & subset2 are present in the transaction set
   -> Instead of thinking in terms of subset1 & subset2, we can combine these and to get a subset (we are not considering the order in which items were bought)
       The constraint for this from above constraints turns out to be:
           2<=|subset|<=N   
 
*/
 
/*
ADT used
   Series: used for storing a single transaction (T1 1 4 3 2)
       col_name: string: name of transaction (transaction id)
       data: vector<int>: transactions (item numbers)
       store_data(col,values) :for storing values
       unique(): returns vector<int> : unique transactions (item numbers)
       get_col_name(),get_data(); return data (non modifiable)
 
   Dataframe: used for  storing  data (vector of Series)
       data: vector<Series> : each row contains transactions
       store_data(data): expects Series object
       operator[]: used to access transaction by name eg: df["T1"]
       columns(): returns transaction names
       unique(): returns unique values for each transaction: returns a Dataframe
       value_counts(): returns frequency count for each transaction (item no)
       get_data(): returns data: non modifiable
  
   FileHandler: used to handle files reading
 
   Left shift (<<) operator has been overloaded for Series & Dataframe objects
 
 
*/


//----------------------------BLACK BOX BEGINS--------------------------------------------
class Series{
protected:
    string col_name;
    vector<int> data;
public:
    friend class Dataframe;
    friend ostream& operator<<(ostream&,const Series&);

    Series();
    Series(const Series &s);
    Series(const string &col,const vector<int> &val);
    void store_data(string col,vector<int> val);
    vector<int> unique()const;
    const string& get_col_name()const;
    const vector<int>& get_data()const;
};
class Dataframe{
private:
    unordered_map<string,int> index_pos;
protected:
    vector<Series> data;
public:
    friend ostream& operator<<(ostream&,const Dataframe&);

    Dataframe();
    void store_data(const Series &data);
    Series& operator[](const string &str);
    vector<string> columns();
    vector<pair<int,int>> value_counts()const;
    Dataframe unique()const;
    const vector<Series>& get_data()const;
    
};
class FileHandler{
private:
    vector<FILE*> files;
    void store_data(Dataframe *df,int i);
public:
    FileHandler();
    Dataframe *read_txt();
    int read_file(char* file_name);
    int read_file(char* file_names[],int no_of_files);
    ~FileHandler();
};



Series::Series(){}
Series::Series(const Series &s){
    this->col_name = s.col_name;
    this->data.reserve(s.data.size());
    for(const int i:s.data){
        this->data.push_back(i);
    }
}
Series::Series(const string &col,const vector<int> &val){
    this->col_name = col;
    this->data.reserve(val.size());
    for(const int i:val){
        this->data.push_back(i);
    }
}
void Series::store_data(string col,vector<int> val){
    this->col_name = col;
    this->data.reserve(val.size());
    this->data.insert(this->data.begin(),val.begin(),val.end());
}

const string& Series::get_col_name()const{
    return this->col_name;
}

const vector<int>& Series::get_data()const{
    return this->data;
}

ostream& operator<<(ostream& stream,const Series& s){
    stream<<s.col_name<<" : ";
    for(const int i:s.data){
        stream<<i<<" ";
    }
    return stream;
}


vector<int> Series::unique() const{
    vector<int> res;
    unordered_set<int> dup;

    for(const int &i:this->data){
        if(dup.find(i)==dup.end()){
            dup.insert(i);
            res.push_back(i);
        }
    }
    return res;
}

Dataframe::Dataframe(){

}

void Dataframe::store_data(const Series &data){
    int index;
    if(this->index_pos.find(data.col_name)==this->index_pos.end()){
        //column is not present
        index = this->data.size();
        this->index_pos.insert({data.col_name,index});
        this->data.push_back(Series());
    }else{
        index = index_pos.find(data.col_name)->second;
    }
    this->data[index].col_name = data.col_name;
    this->data[index].data.reserve(this->data[index].data.size()+data.data.size());
    for(const int i:data.data){
        this->data[index].data.push_back(i);
    }
}


Series& Dataframe::operator[](const string &str){
    assert(this->index_pos.find(str)!=this->index_pos.end());

    int ind = this->index_pos.find(str)->second;
    return this->data[ind];
}




vector<string> Dataframe::columns(){
    vector<string> col;
    for(Series &s:this->data){
        col.push_back(s.col_name);
    }
    return col;
}
vector<pair<int,int>> Dataframe::value_counts()const{
    unordered_map<int,int> index_pos;
    vector<pair<int,int>> count;
    for(const Series &s:this->data){
        for(const int &val:s.data){
            if(index_pos.find(val)==index_pos.end()){
                index_pos.insert({val,count.size()});
                count.push_back({val,1});
            }else{
                int index = index_pos.find(val)->second;
                count[index].second++;
            }
        }
    }
    return count;
}
 Dataframe Dataframe::unique()const{
     Dataframe df;
     for(const Series &s:this->data){
         df.store_data(Series(s.col_name,s.unique()));
     }
     return df;
 }

 const vector<Series>& Dataframe::get_data()const{
     return this->data;
 }

ostream& operator<<(ostream& stream,const Dataframe &df){
    for(const Series &s:df.data){
        stream<<s;
        stream<<endl;
    }
    return stream;
}


FileHandler::FileHandler(){

}

FileHandler::~FileHandler(){
    for(FILE* fin:files){
        if(fin!=stdin)
            fclose(fin);
    }
}
int FileHandler::read_file(char* file_name){
    FILE *ptr = NULL;
    if(strcmp(file_name,".") == 0){
        ptr = stdin;
    }else{
        ptr = fopen(file_name,"r");
        if(ptr==NULL)
            return -1;
    }
    files.push_back(ptr);
    return 0;
}

int FileHandler::read_file(char* file_names[],int no_of_files){
    int chk;
    for(int i=0;i<no_of_files;i++){
        chk = this->read_file(file_names[i]);
        if(chk==-1)
            return -1;
    }
    return 0;
}

void FileHandler::store_data(Dataframe *df,int i){
    assert(i<files.size());
    char c;
    int curr_val;
    FILE* fin = files[i];
    while(true){
        Series s;
        string col_name="";
        vector<int> val;
        curr_val = 0;
        //get column name
        while(true){
            c = fgetc(fin);
            if(c==' '||c=='\n'||c==EOF)
                break;
            col_name+=c;
        }
        if(c==EOF){
            break;
        }
        bool read=false;
        while(true){
            c = fgetc(fin);
            if(c==' '){
                val.push_back(curr_val);
                read = false;
                curr_val = 0;
                continue;
            }else if(c=='\n' || c==EOF)
                break;
            read = true;
            curr_val = curr_val*10 + (c-'0');
        }
        if(read)
            val.push_back(curr_val);

        s.store_data(col_name,val);
        //cout<<s<<endl;
        df->store_data(s);
        if(c==EOF)
            break;
    }
    rewind(fin);
}
Dataframe *FileHandler::read_txt(){
    Dataframe * df= new Dataframe;
    for(int i=0;i<this->files.size();i++){
        this->store_data(df,i);
    }
    return df;
}


template <typename T>
ostream& operator<<(ostream& stream,const vector<T> &V){
    for(const T &val:V){
        stream<<val<<" ";
    }
    return stream;
}

vector<pair<int,int>> get_filtered_items(const vector<pair<int,int>> &frequency_count,const int &frequency_threshold){
    vector<pair<int,int>> items;
    for(const pair<int,int> item_c:frequency_count){
        if(item_c.second>=frequency_threshold){
            items.push_back({item_c.first,item_c.second});
        }
    }
    return items;
}

//----------------------------BLACK BOX ENDS--------------------------------------------


//---------------------Analysis---------------------------------------

vector<string> analyse_subset(int elements,const vector<pair<int,int>> &items,const Dataframe &df){
    vector<string> transactions;
    vector<int> element_index;
    for(int item_no = 0;item_no<items.size();item_no++){
        if(elements & (1<<item_no)){
            element_index.push_back(item_no);
        }
    }

    for(const Series &s: df.get_data()){
        unordered_set<int> elements_seen_so_far;
        const vector<int> &v = s.get_data();
        int i,j;
        i = 0;
        j = 0;
        while(i<element_index.size() && j<v.size()){
            if(elements_seen_so_far.find(items[element_index[i]].first) != elements_seen_so_far.end()){
                i++;
            }else{
                elements_seen_so_far.insert(v[j]);
                j++;
            } 
        }
        while(i<element_index.size() && elements_seen_so_far.find(items[element_index[i]].first) != elements_seen_so_far.end())
            i++;
        //check if all the items is present in this transaction
        if(i==element_index.size()){
            transactions.push_back(s.get_col_name());
        }
    }

    return transactions;
}

void analyse(Dataframe &df){
    //print the transaction
    cout<<"-------------TRANSACTIONS--------------------"<<endl;
    cout<<df;

    //remove the duplicate entries from each transactions
    Dataframe unique_df = df.unique();

    //print the unique transactions
    cout<<"-------------UNIQUE TRANSACTIONS--------------------"<<endl;
    cout<<unique_df;


    //------------------------------Inference from observation 1------------------------------
    //get the frequency count of each items
    vector<pair<int,int>> frequency_count = unique_df.value_counts();

    //print the frequency count
    cout<<"-------------FREQUENCY COUNT--------------------"<<endl;
    for(const pair<int,int> &i:frequency_count){
        cout<<i.first<<" : "<<i.second<<endl;
    }

    //get the max frequency
    int max_frequency = 1;
    for(const pair<int,int> item_c:frequency_count){
        max_frequency = max(max_frequency,item_c.second);
    }

    //set the threshold
    int frequency_threshold = 0;
    int transaction_threshold = 1;

    //get all the items whose frequency count is greater or equal to than the threshold value
    vector<pair<int,int>> items = get_filtered_items(frequency_count,frequency_threshold);
    int N = items.size();
    cout<<"-------------INFERENCE--------------------"<<endl;
    cout<<"\x1B[31mITEMS\t\t\t\t\t\tTRANSACTIONS PRESENT\x1B[0m"<<endl;
    //------------------------------Inference from observation 2------------------------------
    //generate all subsets with constraint defined in observation 2 (last observation)
    //2<=|subset|<=N
    for(int i=2;i<pow(2,N);i++){
        //subset size should be at least 2
        if(ceil(log2(i))!=floor(log2(i))){
            //get all the transactions where all the items in current subset is present
            vector<string> transactions_present = analyse_subset(i,items,df);

            if(transactions_present.size()>=transaction_threshold){
                //print the subset and corresponding transactions
                int item_print_width = 2;
                cout<<"{ ";
                for(int item_no = 0;item_no<N;item_no++){
                    if(i & (1<<item_no)){
                        cout<<frequency_count[item_no].first<<",";
                        item_print_width+=to_string(frequency_count[item_no].first).size() ;
                        item_print_width+=1;
                    }
                }
                cout<<"\b";
                cout<<" }";
                item_print_width += 1;
                //print the space
                for(int i=item_print_width;i<=50;i++)
                    cout<<" ";
                for(string s:transactions_present){
                    cout<<s<<" ";
                }
                cout<<endl;
            }
        }

    }
}

int main(int argc,char *argv[]){
    FileHandler f;
    //load files
    if(argc==1){
        char fname[2] = ".";
        f.read_file(fname);
    }else{
        f.read_file(argv+1,argc-1);
    }
    //read the transactions from files
    Dataframe *df = f.read_txt();
    
    //Analyze the transactions
    analyse(*df);
    return 0;
}
