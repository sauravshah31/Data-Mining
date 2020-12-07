/*
Improve Apriori Algorithm: Partion based apriori
*/
#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <algorithm>
#include <chrono>
using namespace std;


#define NA_INT INT32_MIN
#define NA_CHAR ''



//-----------BLACK BOX-------------
//------------------BLACK BOX-----------------------
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
    Dataframe(const Dataframe &df);
    void store_data(const Series &data);
    void store_data(const string &col_name,const int &value);
    Series& operator[](const string &str);
    vector<string> columns();
    vector<pair<int,int>> value_counts()const;
    Dataframe unique()const;
    const vector<Series>& get_data()const;
    int nrows() const;
    void dropna();
    void fillna(void (*callback)(vector<int> &,void*),const string &col_name,void* callback_params);
    
};

class FileHandler{
public:
    FileHandler();
    int open_file(const char* filename);
    void get_data_chunk(Dataframe *df,int chunk_size);
    int get_npasses();
    int set_nlines(int n);
    int get_nlines() const;
    ~FileHandler();
 
private:
    char* filename;
    FILE* file_ptr=NULL;
    int n_lines=-1;
    int n_passes=0;
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
Dataframe::Dataframe(const Dataframe &df){
    this->index_pos = df.index_pos;
    this->data = df.data;
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
void Dataframe::store_data(const string &col_name,const int &value){
    int index;
    if(this->index_pos.find(col_name)==this->index_pos.end()){
        index = this->data.size();
        this->index_pos.insert({col_name,index});
        this->data.push_back(Series());
        this->data[index].col_name = col_name;
    }else{
        index = index_pos.find(col_name)->second;
    }
    this->data[index].data.push_back(value);
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

 int Dataframe::nrows() const{
     return this->data.size();
 }

  void Dataframe::dropna(){
     //drop the rows containing missing values
    if(this->data.size()==0)
        return;
    int offset = 0;
    int no_of_rows = this->data[0].data.size();
    for(int i=0;i<no_of_rows;i++){
        bool chk = false;
        for(const auto &v:this->data){
            if(v.data[i-offset]==NA_INT){
                chk=true;
                break;
            }
        }

        if(chk==true){
            for(auto &v:this->data){
                v.data.erase(v.data.begin()+i-offset);
            }
            offset++;
        }
    }

 }

 void Dataframe::fillna(void (*callback)(vector<int> &,void *),const string &col_name,void* callback_params=NULL){
    assert( this->index_pos.find(col_name)!=this->index_pos.end());
    int index = this->index_pos.find(col_name)->second;
    (*callback)(this->data[index].data,callback_params);
 }




FileHandler::FileHandler(){    
}

int FileHandler::open_file(const char* filename){
    strcpy(this->filename,filename);
    file_ptr = NULL;
    if(strcmp(filename,".") == 0){
        file_ptr = stdin;
    }else{
        file_ptr = fopen(filename,"r");
    }
    if(file_ptr==NULL)
        return -1;
 
    return 0;
}

/*
Get the data chunk (partition), data is stored in Dataframe* df
*/
void FileHandler::get_data_chunk(Dataframe *df,int chunk_size){
    char c;
    int curr_val;
    while(chunk_size--){
        Series s;
        string col_name="";
        vector<int> val;
        curr_val = 0;
        //get column name
        while(true){
            c = fgetc(this->file_ptr);
            if(c==' '||c=='\n'||c==EOF)
                break;
            col_name+=c;
        }
        if(c==EOF){
            break;
        }
        bool read=false;
        while(true){
            c = fgetc(this->file_ptr);
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

        df->store_data(s);
        if(c==EOF){
            rewind(this->file_ptr);
            this->n_passes++;
            break;
        }
    }
}

int FileHandler::get_npasses(){
    return this->n_passes;
}

int FileHandler::set_nlines(int n){
    //no of lines can be set only once
    if(n_lines!=-1)
        return 0;
    
    n_lines=n;
    return 1;
}

int FileHandler::get_nlines() const{
    return this->n_lines;
}

FileHandler::~FileHandler(){
    if(file_ptr!=NULL){
        fclose(file_ptr);
    }
}


ostream& operator<<(ostream& stream,const Dataframe &df){
    for(const Series &s:df.data){
        stream<<s;
        stream<<endl;
    }
    return stream;
}

template <typename T>
ostream& operator<<(ostream& stream,const vector<T> &V){
    for(const T &val:V){
        stream<<val<<" ";
    }
    return stream;
}

//-------------------BLACK BOX ENDS---------------



//-------------APRIORI BEGINS--------------------
//generate frequent itemset using apriori
//this algorith assumes the itemset is in the main memory (struct Dataframe)

int get_itemset_count(const Dataframe &df,const vector<int> &itemset){
    /*
    Get the count of itemset (transaction itemset) in the dataset
    */
    int curr_tcount = 0;
    for(const Series &s:df.get_data()){
        unordered_set<int> seen_items;
        const vector<int> curr_data = s.get_data();
        int i,j;
        i=0;
        j=0;
        while(i<itemset.size() && j<curr_data.size()){
            if(itemset[i] == curr_data[j]){
                i++;j++;
            }else if(seen_items.find(itemset[i])!=seen_items.end()){
                i++;
            }else{
                seen_items.insert(curr_data[j]);
                j++;
            }
        }
        while(i<itemset.size() && seen_items.find(itemset[i])!=seen_items.end()){
            i++;
        }
        if(i==itemset.size()){
            curr_tcount++;
        }
    }
    return curr_tcount;
}
int get_itemset_count(const Dataframe &df,const vector<int> &itemset,const vector<int> &index){
    /*
    Get the count of subset of itemset (transaction itemset) specified by index number in the dataset
    */
    int curr_tcount = 0;
    for(const Series &s:df.get_data()){
        unordered_set<int> seen_items;
        const vector<int> curr_data = s.get_data();
        int i,j;
        i=0;
        j=0;
        while(i<index.size() && j<curr_data.size()){
            if(itemset[index[i]] == curr_data[j]){
                i++;j++;
            }else if(seen_items.find(itemset[index[i]])!=seen_items.end()){
                i++;
            }else{
                seen_items.insert(curr_data[j]);
                j++;
            }
        }
        while(i<index.size() && seen_items.find(itemset[index[i]])!=seen_items.end()){
            i++;
        }
        if(i==index.size()){
            curr_tcount++;
        }
    }
    return curr_tcount;
}

bool check_prefix(const vector<int> &itemset1,const vector<int> &itemset2, int k){
    for(int i=1;i<k;i++){
        if(itemset1[i-1]!=itemset2[i-1]){
            return false;
        }
    }
    return true;
}

//Join and Prune Step
//generate frequent itemset using apriori
vector<vector<int>> frequent_itemset_using_apriori(Dataframe &df,const int min_support){
    vector<vector<int>> result;
    vector<vector<int>> Ck,Lk;
    int k;
    bool flag;

    vector<pair<int,int>> frequency_count = df.value_counts();

    //---------PRUNE STEP-------------
    //for 1 itemset, use apriori heuristic to get L1(Lk)
    for(pair<int,int> fc:frequency_count){
        if(fc.second>=min_support){
            Lk.push_back({fc.first});
        }
    }

    result.reserve(result.size()+Lk.size());
    result.insert(result.end(),Lk.begin(),Lk.end());

    //genetake k>=2 itemsets
    k=1;
    while(Lk.size()!=0){
        Ck = Lk;
        Lk.clear();
        for(int i=0;i<Ck.size();i++){
            for(int j=i+1;j<Ck.size();j++){

                ////---------JOIN STEP-------------
                flag = check_prefix(Ck[i],Ck[j],k);
                //cout<<Ck[i]<<" | "<<Ck[j]<<" "<<boolalpha<<flag<<endl;
                if(flag==true){
                    //apriori heuristic satisfied
                    //append first k-1 common elements
                    vector<int> temp;
                    for(int t=1;t<k;t++){
                        temp.push_back(Ck[i][t-1]);
                    }

                    //append remaining items from Ck[i]
                    for(int t=k-1;t<Ck[i].size();t++){
                        temp.push_back(Ck[i][t]);
                    }

                    //append remaining items from Ck[j]
                    for(int t=k-1;t<Ck[j].size();t++){
                        temp.push_back(Ck[j][t]);
                    }

                    //---------PRUNE STEP-------------                    
                    //check if itemset if frequent
                    int ntransactions = get_itemset_count(df,temp);
                    if(ntransactions>=min_support)
                        Lk.push_back(temp);
                }
            }
        }
        //add Lk to result
        result.reserve(result.size()+Lk.size());
        result.insert(result.end(),Lk.begin(),Lk.end());
        k++;
    }
    return result;
}

//-------------APRIORI ENDS----------------------



//-----------Partition base apriori begins---------
/*
    * The entire database is divided into number of partition
    PHASE-1
    -----------
        * For each partition, we store the data in the main memory and generate the frequent itemset using apriori
        * The union of frequent itemset from all of these partitions gives the potential frequent itemset for the entire dataset. This union
          can contains itemset which are not frequent, but will not miss any itemset which are frequent
    PHASE-2
    -----------
        * The count for each of the itemset generated in phase-I is checked, and compared with global frequent itemset
*/

vector<vector<int>> generate_frequent_itemset_phase1(FileHandler *fptr,const float min_support,const int partition_size){
    vector<vector<int>> result;
    set<vector<int>> result_util;
    int n_transactions=0; //total no of transactions in the database;
    
    //read the databse ,in chunk, untill one  pass is made
    while(fptr->get_npasses()==0){
        Dataframe* df=new Dataframe();

        fptr->get_data_chunk(df,partition_size);
        int curr_psize;
        int min_support_count;
        
        curr_psize = df->nrows();
        n_transactions += curr_psize;
        min_support_count = ceil((min_support/100) * df->nrows());
        vector<vector<int>> curr_frequent_itemset = frequent_itemset_using_apriori(*df,min_support_count);

        //add current partition's frequent itemsets to the result
        for(vector<int>& itemset:curr_frequent_itemset){
            result_util.insert(itemset);
        }
        delete df;

    }

    //set the number of transactions
    fptr->set_nlines(n_transactions);

    //convert set to vector
    for(const vector<int> &itemset:result_util)
        result.push_back(itemset);

    return result;
}

vector<vector<int>> generate_frequent_itemset_phase2(FileHandler *fptr,vector<vector<int>> candidature_set,const float min_support,const int partition_size){
    vector<vector<int>> result;

    int ntransactions = fptr->get_nlines();
    int min_support_count = ceil((min_support/100) * ntransactions);
    int curr_npasses = fptr->get_npasses();

    vector<int> count(candidature_set.size(),0); //store count of each itemset

    assert(ntransactions!=-1);
    assert(fptr->get_npasses()>=1);

    //read the transactions on chunk and increment the count of itemset
    while(fptr->get_npasses()==curr_npasses){
        Dataframe *df = new Dataframe();
        fptr->get_data_chunk(df,partition_size);
        for(int i=0;i<candidature_set.size();i++){
            int curr_count = get_itemset_count(*df,candidature_set[i]);
            count[i]+=curr_count;
        }
        delete df;
    }

    for(int i=0;i<candidature_set.size();i++){
        if(count[i]>=min_support_count){
            result.push_back(candidature_set[i]);
        }
    }

    return result;
}

vector<vector<int>> generate_frequent_itemset(FileHandler *fptr,const float min_support,const int partition_size){
    vector<vector<int>> result;

    result = generate_frequent_itemset_phase1(fptr,min_support,partition_size);
    result = generate_frequent_itemset_phase2(fptr,result,min_support,partition_size);

    return result;
}
//-----------Partition base apriori ends---------


//-----------









int main(int argc,char *argv[]){
    assert(argc>=2);

    FileHandler f;

    f.open_file(argv[1]);
    float min_support = 30.0;
    int partition_size = 3;

    vector<vector<int>> frequent_itemset = generate_frequent_itemset(&f,min_support,partition_size);
    set<vector<int>> result;
    for(auto &i:frequent_itemset){
        sort(i.begin(),i.end());
        result.insert(i);
    }
    cout<<"-------FREQUENT ITEMSET USING 30% MIN SUPPORT------"<<endl;
    
    for(auto &it:result){
        cout<<it<<endl;
    }

}