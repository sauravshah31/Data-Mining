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
#include <list>
#include <chrono>
#include <queue>
using namespace std;


#define NA_INT INT32_MIN
#define NA_CHAR ''

#define CHUNK_SIZE 5


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
    //strcpy(this->filename,filename);
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

//-------------------BLACK BOX ENDS---------------


//-------------------DIC BEGINS---------------------

#define DC 0 //dashed circle
#define DS 1 //dashed square
#define SC 2 //solid circle
#define SS 3 //solid square


typedef struct trie_node{
    int item_id;
    int item_count;
    int nread_db_left; //for checking if entire datase has been read for this node (transaction)
    struct trie_node **children;
    int flag;
    struct trie_node *parent;


    trie_node(const int N,const int db_size){
        item_count = 0;
        nread_db_left = db_size;
        this->children = new trie_node*[N+1];
        for(int i=0;i<=N;i++)
            this->children[i]=NULL;
        flag = DC;
    }
}trie_node;

class Trie{
public:
    trie_node* insert_util(trie_node *root,const vector<int> &transaction);
    Trie(const int N,const int db_size);
    trie_node* insert(const vector<int> &transaction);
    int get_nchild()const;
public:
    trie_node *root=NULL;
private:
    int n_child;
    int db_size; 
};


unordered_set<trie_node *> ITEM_PTR[4];
/*
ITEM_PTR[0] -> dashed circle list 
ITEM_PTR[1] -> dashed square list
ITEM_PTR[2] -> solid circle list
ITEM_PTR[3] -> solid square list
*/

/*
Functions for handling DIC
*/

/*print utiliti*/
void print_parent(trie_node* T);
void print(Trie *T);
void print_item();
void get_parent(trie_node* T,vector<int> &result);

/*Get one itemset*/
void get_one_itemset(FileHandler* fptr,vector<int> &result);
/*Add one itemset to dashed circle*/
void init_DC(Trie *T,const vector<int> &items);
/*increment Count after reading transaction*/
void increment_count(Trie *T,const vector<int> &item,int i);
/*form superset of items ,ie L(K+1) from L(K)*/
void form_superset(Trie *T);
/*Increment count of item after one pass of database*/
void increment_count(trie_node *root,const vector<int> &item,int i);
/*Check if DC can be converted to DS*/
void check_DC_to_DS(int min_support_count);
/*After each pass, decrement no o pass left before complete db has been read*/
void decrement_nread_db_left(int factor);
/*Check if dashed box can be converted to solid box*/
void check_dashed_to_solid();
/*All the steps of DIC algorithm, returns frequent itemset*/
vector<vector<int>> dic_algo(FileHandler* fptr,const unordered_map<string,float> kwargs);




Trie::Trie(const int N,const int db_size){
    this->n_child = N;
    this->db_size = db_size;
    this->root = new trie_node(this->n_child,this->db_size);
    this->root->item_id = -1;
    this->root->parent = NULL;
}

trie_node* Trie::insert_util(trie_node *root,const vector<int> &transaction){
    for(int i=0;i<transaction.size();i++){
        
        if(root->children[transaction[i]]==NULL){
            root->children[transaction[i]] = new trie_node(this->n_child,this->db_size);
            root->children[transaction[i]]->item_id = transaction[i];
            root->children[transaction[i]]->nread_db_left = this->db_size;
            root->children[transaction[i]]->parent = root;
        }
        root = root->children[transaction[i]];
    }
    return root;
}
trie_node* Trie::insert(const vector<int> &transaction){
    return this->insert_util(this->root,transaction);
}

int Trie::get_nchild()const{
    return this->n_child;
}


void print(Trie* T){
    cout<<"--------Trie---------"<<endl;
    queue<trie_node*> Q;
    Q.push(T->root);

    Q.push(NULL);
    while(Q.front()!=NULL){
        while(Q.front()!=NULL){
            trie_node* curr = Q.front();
            for(int i=0;i<=T->get_nchild();i++){
                if(curr->children[i]){
                    cout<<"["<<curr->children[i]->item_id<<":"<<curr->children[i]->item_count<<"] "<<" ";
                    Q.push(curr->children[i]);
                }
            }
            Q.pop();
        }
        Q.pop();
        Q.push(NULL);
        cout<<endl;
    }
}

void print_parent(trie_node* t){
    t = t->parent;
    while(t!=NULL && t->parent!=NULL){
        cout<<t->item_id;
        t = t->parent;
    }
}
void get_parent(trie_node* t,vector<int> &result){
    t = t->parent;
    while(t!=NULL && t->parent!=NULL){
        result.push_back(t->item_id);
        t = t->parent;
    }
}
void print_item(){
    cout<<"DC :";
    for(auto i: ::ITEM_PTR[DC]){
        cout<<i->item_id;
        print_parent(i);
        cout<<" ";
    }
    cout<<endl;
    cout<<"DS :";
    for(auto i: ::ITEM_PTR[DS]){
        cout<<i->item_id;
        print_parent(i);
        cout<<" ";
    }
    cout<<endl;
    cout<<"SC :";
    for(auto i: ::ITEM_PTR[SC]){
        cout<<i->item_id;
        print_parent(i);
        cout<<" ";
    }
    cout<<endl;
    cout<<"SS :";
    for(auto i: ::ITEM_PTR[SS]){
        cout<<i->item_id;
        print_parent(i);
        cout<<" ";
    }
    cout<<endl;
}


void get_one_itemset(FileHandler* fptr,vector<int> &result){
    unordered_set<int> unique_values;
    int total_transaction = 0;

    int curr_pass = fptr->get_npasses();
    //read the database in chunk, for getting 1 itemset
    while(fptr->get_npasses()==curr_pass){
        Dataframe *df = new Dataframe;
        fptr->get_data_chunk(df,CHUNK_SIZE);
        
        total_transaction += df->nrows();
        //add current partition values to the set
        vector<pair<int,int>> item_count = df->value_counts();
        for(int i=0;i<item_count.size();i++){
            unique_values.insert(item_count[i].first);
        }
    }

    //set the no of transactions
    fptr->set_nlines(total_transaction);
    result.reserve(unique_values.size());
    for(const int &i:unique_values)
        result.push_back(i);
}


void init_DC(Trie *T,const vector<int> &items){
    for(const int &i:items){
        //insert 1 itemset into the Trie
        trie_node* curr_node =  T->insert({i});

        //put the current node in Dashed Circle
        ::ITEM_PTR[DC].insert(curr_node);
    }
}

void form_superset(Trie *T){
    //generate higher itemset from items from DS and SS
    trie_node* temp;
    int i,j;
    i=0;
    unordered_set<trie_node *>::iterator item1_itr, item2_itr;
    item1_itr = ::ITEM_PTR[DS].begin();
    
    for(i=0;i<::ITEM_PTR[DS].size();i++){

        item2_itr = ::ITEM_PTR[DS].begin();
        advance(item2_itr,i+1);
        for(item2_itr;item2_itr != ::ITEM_PTR[DS].end(); item2_itr++){
            trie_node *item1,*item2;
            item1 = *(item1_itr);
            item2 = *(item2_itr);
            
            //for L(K+1) check if L(K) is same
            if(item1->parent==item2->parent){
                //condition satistifed (same like join condition, prefix one)
                if(item1->item_id < item2->item_id){
                    temp = T->insert_util(item1,{item2->item_id});
                }else{
                    temp = T->insert_util(item2,{item1->item_id});
                }
                ITEM_PTR[DC].insert(temp);
            }
        }

        item2_itr = ::ITEM_PTR[SS].begin();
        for(item2_itr;item2_itr != ::ITEM_PTR[SS].end(); item2_itr++){
            trie_node *item1,*item2;
            item1 = *(item1_itr);
            item2 = *(item2_itr);
            
            //for L(K+1) check if L(K) is same
            if(item1->parent==item2->parent){
                //condition satistifed (same like join condition, prefix one)
                if(item1->item_id < item2->item_id){
                    temp = T->insert_util(item1,{item2->item_id});
                }else{
                    temp = T->insert_util(item2,{item1->item_id});
                }
                ITEM_PTR[DC].insert(temp);
            }
        }
        
        item1_itr++;
    }

    item1_itr = ::ITEM_PTR[SS].begin();
    for(i=0;i<::ITEM_PTR[SS].size();i++){

        item2_itr = ::ITEM_PTR[SS].begin();
        advance(item2_itr,i+1);
        for(item2_itr;item2_itr != ::ITEM_PTR[SS].end(); item2_itr++){
            trie_node *item1,*item2;
            item1 = *(item1_itr);
            item2 = *(item2_itr);
            
            //for L(K+1) check if L(K) is same
            if(item1->parent==item2->parent){
                //condition satistifed (same like join condition, prefix one)
                if(item1->item_id < item2->item_id){
                    temp = T->insert_util(item1,{item2->item_id});
                }else{
                    temp = T->insert_util(item2,{item1->item_id});
                }
                ITEM_PTR[DC].insert(temp);
            }
        }
        
        item1_itr++;
    }
}


void increment_count(trie_node *root,const vector<int> &item,int i=0){
    if(root==NULL)
        return;

    for(int s=i;s<item.size();s++){
        trie_node * curr = root->children[item[s]];
        if(curr){
            if(::ITEM_PTR[SS].find(curr)==::ITEM_PTR[SS].end() && ::ITEM_PTR[SC].find(curr)==::ITEM_PTR[SC].end())
                curr->item_count++;
            increment_count(curr,item,i+1);
        }
    }
}

void check_DC_to_DS(int min_support_count){
    int offset = 0;
    int s = ::ITEM_PTR[DC].size();
    for(int i=0;i<s;i++){
        auto it = ::ITEM_PTR[DC].begin();
        advance(it,i-offset);
        trie_node* curr_node = *it;
        if(curr_node->item_count>=min_support_count){
            ::ITEM_PTR[DC].erase(it);
            ::ITEM_PTR[DS].insert(curr_node);
            offset++;
        }
    }

}


void decrement_nread_db_left(int factor){
    for(trie_node* curr_node : ::ITEM_PTR[DC]){
        curr_node->nread_db_left -= factor;
    }
    for(trie_node* curr_node : ::ITEM_PTR[DS]){
        curr_node->nread_db_left -= factor;
    }

}

void check_dashed_to_solid(){
    int offset = 0;
    int s = ::ITEM_PTR[DC].size();
    for(int i=0;i<s;i++){
        auto it = ::ITEM_PTR[DC].begin();
        advance(it,i-offset);
        trie_node* curr_node = *it;
        if(curr_node->nread_db_left<=0){
            ::ITEM_PTR[DC].erase(it);
            ::ITEM_PTR[SC].insert(curr_node);
            offset++;
        }
    }
    offset = 0;
    s = ::ITEM_PTR[DS].size();
    for(int i=0;i<s;i++){
        auto it = ::ITEM_PTR[DS].begin();
        advance(it,i-offset);
        trie_node* curr_node = *it;
        if((curr_node->nread_db_left)<=0){
            ::ITEM_PTR[DS].erase(it);
            ::ITEM_PTR[SS].insert(curr_node);
            offset++;
        }
    }
}


vector<vector<int>> dic_algo(FileHandler* fptr, float min_support){
    vector<vector<int>> frequent_itemset;

    vector<int> one_itemset;
    Trie * T;

    get_one_itemset(fptr,one_itemset);
    T = new Trie(one_itemset.size(),fptr->get_nlines());
    T->root->parent = NULL;


    init_DC(T,one_itemset);
    int pass_no = 0;
    cout<<"-------PASS "<<pass_no<<"-----------"<<endl;
    print(T);
    print_item();
    cout<<"---------------------------------------"<<endl<<endl;

    //const int min_support_count = ceil((it->second/100)*fptr->get_nlines());
    const int min_support_count = ceil((min_support/100)*fptr->get_nlines());
    while(::ITEM_PTR[DC].size()!=0 || ::ITEM_PTR[DS].size()!=0){
        //read M transactons
        Dataframe *df = new Dataframe();
        fptr->get_data_chunk(df,CHUNK_SIZE);
        vector<string> columns = df->columns();
        for(auto col:columns){
            //increment counter for Dashed items (DC, DS)
            auto item = (*df)[col].get_data();
            increment_count(T->root,item);
        }

        //now decrement the nread__db_left, as a pass through the database has been made
        decrement_nread_db_left(df->nrows());
        
        //now check if items in DC can be put into DS
        check_DC_to_DS(min_support_count);

        //now check if any immediate superset can be formed
        form_superset(T);

        //check if dashed can be converted to solid
        check_dashed_to_solid();

        pass_no += 1;
        cout<<"-------PASS "<<pass_no<<"-----------"<<endl;
        print(T);
        print_item();
        cout<<"---------------------------------------"<<endl<<endl;
    }

    //SS contains frequent itemset
    for(auto it:ITEM_PTR[SS]){
        vector<int> curr_item = {it->item_id};
        get_parent(it,curr_item);
        frequent_itemset.push_back(curr_item);
    }

    return frequent_itemset;
}

int main(int argc,char *argv[]){
    assert(argc>=2);

    FileHandler *fptr;

    fptr = new FileHandler();
    fptr->open_file(argv[1]);

    
    float min_confidence = 30.0; //30% confidence
    cout<<"-------Frequent ITEMSET AT 30% MIN CONFIDENCE (WITH STEPS IN DIC)-------"<<endl;
    vector<vector<int>> frequent_itemset = dic_algo(fptr,min_confidence);

    cout<<"-------------FREQUENT ITEMSET GENERATED-----------------"<<endl;
    for(auto item:frequent_itemset){
        cout<<item<<endl;
    }
    return 0;
}
