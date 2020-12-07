/*
Handling Missing Values And noise

● Compile and run the binary
● ./a.out transaction_file.csv
● The file must have csv(comma separated values) and the encoding must be ‘ASCII’

eg:
./a.out test.csv
*/

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
using namespace std;


#define NA_INT INT32_MIN
#define NA_CHAR ''


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



//--------DATA PREPROCESSING--------------------

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


//use different techniques to handle missing values
void handle_missing_values(Dataframe &df){
    vector<string> cols = df.columns();
    cout<<"--Handling Missing Data--"<<endl;
    //original data
    cout<<"-------ORIGINAL DATA----------"<<endl;
    cout<<df;

    //drop row with missing values
    Dataframe dropna_df = Dataframe(df);
    cout<<"---After Droping Rows With Missing Values---"<<endl;
    dropna_df.dropna();
    cout<<dropna_df;

    //fill with zero
    Dataframe df_test2 = Dataframe(df);
    cout<<"--After Replacing with 5---"<<endl;
    int *constant = new int;
    (*constant) = 5;
    for(const string &s:cols)
        df_test2.fillna(&fill_with_constant,s,(void*)constant);
    cout<<df_test2;

    //fill with mean
    Dataframe df_test3 = Dataframe(df);
    cout<<"--After Replacing with mean---"<<endl;
    for(const string &s:cols)
        df_test3.fillna(&fill_with_mean,s);
    cout<<df_test3;

    //fill with median
    Dataframe df_test4 = Dataframe(df);
    cout<<"--After Replacing with median---"<<endl;
    for(const string &s:cols)
        df_test4.fillna(&fill_with_median,s);
    cout<<df_test4;

    //forward propagate the values
    Dataframe df_test5 = Dataframe(df);
    cout<<"--After Propagating the last known values forward---"<<endl;
    for(const string &s:cols)
        df_test5.fillna(&ffill,s);
    cout<<df_test5;

    //backward propagate the values
    Dataframe df_test6 = Dataframe(df);
    cout<<"--After Propagating the last known values backwards---"<<endl;
    for(const string &s:cols)
        df_test6.fillna(&bfill,s);
    cout<<df_test6;
}



void binning_techniques(Dataframe df,string col_name,int bins){
    cout<<endl;
    vector<int> V;
    const vector<int> temp = df[col_name].get_data();
    V.reserve(temp.size());
    for(int i:temp){
        V.push_back(i);
    }
    cout<<"--Binning Techniques--"<<endl;
    V = {8,4,21,24,15,21,15,8,34,39,40};
    cout<<"original data: "<<V<<endl;
    //sort the values
    sort(V.begin(),V.end());
    cout<<"Sorted data: "<<V<<endl;
    //partition into (equal-frequency) bins:
    cout<<"----Partition into (equal-frequency) bins----"<<endl;
    for(int i=0;i<ceil(V.size()/(bins+0.0));i++){
        cout<<"bin "<<(i+1)<<": ";
        for(int j=i*bins;j<min((i*bins)+bins,(int)V.size());j++){
            cout<<V[j]<<", ";
        }
        cout<<"\b\b "<<endl;
    }


    //smootheing by bin means
    cout<<"------Smoothing by bin means------"<<endl;
    for(int i=0;i<ceil(V.size()/(bins+0.0));i++){
        int total,n;
        total=n=0;
        cout<<"bin "<<(i+1)<<": ";
        for(int j=i*bins;j<min((i*bins)+bins,(int)V.size());j++){
            total+=V[i];
            n++;
        }
        int mean = total/n;
        for(int j=i*bins;j<min((i*bins)+bins,(int)V.size());j++){
            cout<<mean<<", ";
        }
        cout<<"\b\b "<<endl;
    }

    //smoothening by bin boundaries
    cout<<"------Smoothing by bin boundaries------"<<endl;
    for(int i=0;i<ceil(V.size()/(bins+0.0));i++){
        cout<<"bin "<<(i+1)<<": ";
        int start = i*bins;
        int end = min((i*bins)+bins,(int)V.size())-1;
        for(int j=start;j<=end;j++){
            int curr = (V[j]-V[start])<(V[end]-V[j])?V[start]:V[end];
            cout<<curr<<", ";
        }
        cout<<"\b\b "<<endl;
    }
}

int main(int argc,char *argv[]){
    assert(argc>=2);
    FileHandler f;
    f.read_file(argv[1]);

    Dataframe *df = f.read_csv();

    handle_missing_values(*df);
    binning_techniques(*df,"col1",3);
}
