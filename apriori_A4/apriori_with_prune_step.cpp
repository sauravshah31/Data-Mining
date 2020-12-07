#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <algorithm>
#include <chrono>
using namespace std;


#define NA_INT INT32_MIN
#define NA_CHAR ''

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
    void dropna();
    void fillna(void (*callback)(vector<int> &,void*),const string &col_name,void* callback_params);
    
};
class FileHandler{
private:
    vector<FILE*> files;
    void store_data_txt(Dataframe *df,int i);
    void store_data_csv(Dataframe *df,int i);
public:
    FileHandler();
    Dataframe *read_txt();
    Dataframe *read_csv();
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

void FileHandler::store_data_txt(Dataframe *df,int i){
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

void FileHandler::store_data_csv(Dataframe *df,int i){
    assert(i<files.size());
    char c;
    FILE* fin = files[i];

    vector<string> col_names;

    string curr_col_name;
    string curr_val;
    int curr_col_ind;

    auto preprocess = [](string &str) -> string {
        int i=0;
        int j=str.size()-1;
        while(i<=j && str[i]==' ')
            i++;
        while(j>=i && str[j]==' ')
            j--;
        
        return str.substr(i,j+1);
    };
    c = ' ';
    //skip the blank lines
    while(true){
        c = fgetc(fin);
        if(c!='\n')
            break;
    }

    curr_col_name = c;
    //get column names from first row
    while(true){
        c = fgetc(fin);
        if(c == EOF || c == '\n'){
            col_names.push_back(preprocess(curr_col_name));
            break;
        }else if(c==','){
            col_names.push_back(preprocess(curr_col_name));
            curr_col_name = "";
        }else{
            curr_col_name += c;
        }
    }
    
    curr_col_ind = 0;
    curr_val.clear();
    //get the values
    while(true){
        c = fgetc(fin);
        if(c==',' || c==EOF || c=='\n'){
            curr_val = preprocess(curr_val);
            int val = 0;
            int i = 0;
            if(curr_val==""){
                df->store_data(col_names[curr_col_ind],NA_INT);
            }else{
                
                df->store_data(col_names[curr_col_ind],stoi(curr_val));
            }
            curr_col_ind++;
            curr_val.clear();
        }else{
            curr_val.push_back(c);
        }

        if(c==EOF)
            break;
        if(c=='\n'){
            //fill the remaining values with NA
            while(curr_col_ind<col_names.size()){
                df->store_data(col_names[curr_col_ind++],NA_INT);
            }
            curr_col_ind = 0;
        }
    }
    rewind(fin);
}
Dataframe *FileHandler::read_txt(){
    Dataframe * df= new Dataframe;
    for(int i=0;i<this->files.size();i++){
        this->store_data_txt(df,i);
    }
    return df;
}

Dataframe *FileHandler::read_csv(){
    Dataframe * df= new Dataframe;
    for(int i=0;i<this->files.size();i++){
        this->store_data_csv(df,i);
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

//------------------BLACK BOX ENDS-----------------------



//--------DATA PREPROCESSING TECHNIQUES--------------------

bool cmp_asc(const int &a,const int &b){
    return a>=b;
}

int partition(vector<int> &V,int l,int r,bool(*cmp)(const int&,const int &)){
    int random_pos = rand()%(r-l+1) +l;
    swap(V[r],V[random_pos]);
    int i,j;
    i=l,j=l;
    while(j<r){
        if(!(*cmp)(V[j],V[r])){
            swap(V[i],V[j]);
            i++;
        }
        j++;
    }
    swap(V[i],V[r]);
    return i;
}

int kth_smallest(vector<int> &V,int l,int r,bool(*cmp)(const int&,const int &),const int k){
    while(l<r){
        int p = partition(V,l,r-1,cmp);
        if((p+1)==k){
            return V[p];
        }else if((p+1)>k){
            r = p;
        }else{
            l = p+1;
        }
    }
    return NA_INT;
}


//fill the missing value with given constant
void fill_with_constant(vector<int> &V,void *constant){
    int c = *((int*)constant);
    for(int &val:V){
        if(val==NA_INT){
            val = c;
        }
    }
}

//propagate last non-missing value forward
void ffill(vector<int> &V,void* _){
    int last_seen = NA_INT;
    for(int &val:V){
        if(val==NA_INT){
            val = last_seen;
        }
        if(val!=NA_INT)
            last_seen = val;
    }
}

//propagate last non-missing value backward
void bfill(vector<int> &V,void *_){
    int last_seen = NA_INT;
    for(int i=V.size()-1;i>=0;i--){
        if(V[i]==NA_INT){
            V[i] = last_seen;
        }
        if(V[i]!=NA_INT)
            last_seen = V[i];
    }
}

//replace the missing values with mean
void fill_with_mean(vector<int> &V,void *_){
    int total = 0;
    int total_n = 0;

    vector<int*> na_pos;
    na_pos.reserve(V.size()/4);

    for(int &val:V){
        if(val==NA_INT){
            na_pos.push_back(&val);
        }else{
            total += val;
            total_n++;
        }
    }

    int mean = total/total_n;

    for(int *&pos:na_pos){
        (*pos) = mean;
    }
}

//replace the missing values with median
void fill_with_median(vector<int> &V,void* _){
    vector<int*> na_pos;
    vector<int> values;

    na_pos.reserve(V.size()/4);
    values.reserve(V.size()/2);
    for(int &val:V){
        if(val==NA_INT){
            na_pos.push_back(&val);
        }else{
            values.push_back(val);
        }
    }

    int median = kth_smallest(values,0,values.size(),cmp_asc,values.size()/2);

    for(int* &pos:na_pos){
        (*pos) = median;
    }
}
//--------DATA PREPROCESSING TECHNIQUES  ENDS--------------------


//----------APRIORI BEGINS----------------
/*
OBSERVATIONS:
* If a itemset is infrequent, the higher itemsets containg this itemset will also be infrequent,
  so don't consider this itemset for further higher itemset generation -> purne step
* How not to generate duplicate k itemset? One way is to check if the current itemset already exists, 
  but it takes a lot computation to perform this, Can we improve this further?
   -> We use prefix property for this, for k itemset, while considering k-1 subset we check if they have (k-1) same prefix,
      we add the k itemset to out list only if this condition is satisfied
      While performing union of two (k-1) subset, we simply append the first k prefix(which are same),
      then append the remaining items from first itemset followed by the second dataset
*/

int get_itemset_count(const Dataframe &df,const vector<int> itemset){
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

vector<pair<vector<int>,vector<int>>> genetate_association_rule(Dataframe &df,vector<vector<int>> frequent_itemset,const int min_confidence){
    vector<pair<vector<int>,vector<int>>> relation;


    vector<int> set1_index;
    vector<int> set2_index;

    int confidence;
    for(const vector<int>& item:frequent_itemset){
        //now generate all possible two subset from itemset and compare it's confidence with min_confidence
        //each subset must have at least one item =>  2^n-2 > i < 0
        for(int i=1;i<pow(2,item.size())-1;i++){
            set1_index.clear();
            set2_index.clear();
            for(int j=0;j<item.size();j++){
                if(i & (1<<j)){
                    set1_index.push_back(j);
                }else
                {
                    set2_index.push_back(j);
                }
                
            }

            confidence = get_itemset_count(df,item,set1_index);
            if(confidence>=min_confidence){
                pair<vector<int>,vector<int>> curr;

                curr.first.reserve(set1_index.size());
                for(const int &t:set1_index){
                    curr.first.push_back(item[t]);
                }

                curr.second.reserve(set2_index.size());
                for(const int &t:set2_index){
                    curr.second.push_back(item[t]);
                }

                relation.push_back(curr);
            }
        }
    }

    return relation;
}

void mine_data(Dataframe &df,const unordered_map<string,float> kwargs){
    //df contains the input data
    //kwargs contains other addition data like min_support, min_confidence

    vector<string> columns = df.columns();
    const int ntransactions = columns.size();

    //get the min_support % and then convert into the a number
    assert(kwargs.find("min_support")!=kwargs.end());
    const int min_support = ceil((kwargs.find("min_support")->second/100) * ntransactions);

    vector<vector<int>> frequent_itemset = frequent_itemset_using_apriori(df,min_support);

    cout<<"--------THE GENERATED FREQUENT ITEMSET ARE----------"<<endl;
    for(vector<int> &v:frequent_itemset){
        cout<<v<<endl;
    }

    //Now generate the association rule using the frequent itemset and min_count
    assert(kwargs.find("min_confidence")!=kwargs.end());
    const int min_confidence = ceil((kwargs.find("min_confidence")->second/100) * ntransactions);
    vector<pair<vector<int>,vector<int>>> association_rule = genetate_association_rule(df,frequent_itemset,min_confidence);
    cout<<"--------THE GENERATED ASSOCIATION RULES ARE----------"<<endl;
    for(const pair<vector<int>,vector<int>> &relation:association_rule){
        cout<<"{ "<<relation.first<<"} => { "<<relation.second<<"}"<<endl;
    }
}



int main(int argc,char *argv[]){
    assert(argc>=2);
    FileHandler f;
    f.read_file(argv+1,argc-1);

    Dataframe *df = f.read_txt();
    *df = df->unique(); //check for duplicate transactions in a transaction
    
    cout<<"-----INPUT DATASET-----"<<endl;
    cout<<*df<<endl;

    unordered_map<string,float> kwargs;
    kwargs["min_confidence"] = 50.0;

    auto start =chrono::high_resolution_clock::now();

    cout<<"---TAKING MIN SUPPORT 20%-----"<<endl;
    kwargs["min_support"] = 20.0;
    mine_data(*df,kwargs);
    cout<<endl;

    
    cout<<"---TAKING MIN SUPPORT 30%-----"<<endl;
    kwargs["min_support"] = 40.0;
    mine_data(*df,kwargs);
    cout<<endl;

    cout<<"---TAKING MIN SUPPORT 50%-----"<<endl;
    kwargs["min_support"] = 60.0;
    mine_data(*df,kwargs);
    cout<<endl;

    auto stop =chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(stop-start);
    cout<<"Total Time of execution is : "<<duration.count()<<" milli seconds"<<endl;
    /*
    cout<<"---TAKING MIN SUPPORT 0%-----"<<endl;
    kwargs["min_support"] = 0.0;
    general_association_rule(*df,kwargs);
    cout<<endl;
    */   
}
