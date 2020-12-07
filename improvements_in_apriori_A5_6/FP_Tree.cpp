/*
Generate FP Tree
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
#include <queue>
using namespace std;


#define NA_INT INT32_MIN
#define NA_CHAR ''

#define CHUNK_SIZE 1000


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

//-------------------BLACK BOX ENDS---------------




//----------------FP Tree BEGINS--------------

typedef struct fptree_node{
    int item_id;
    int item_count;
    struct fptree_node *parent;
    struct fptree_node *next;
    vector<struct fptree_node*> children;
}fptree_node;

typedef struct header_node{
    int item_id;
    int item_count;
    fptree_node *item_ptr;
}header_node;


//Header table
class Header{
public:
    Header(const int nitems);
    void insert(const vector<pair<int,int>> &item_count);
    header_node* get_node(const int item_id);
    void print();
public:
    header_node **items;
private:
    int *item_pos;
    int curr_n;
    int total_n;
};


/*utility function for sorting 1 itemset*/
bool cmp_pair_des(const pair<int,int> &a,const pair<int,int> &b);

/*functions for handling FP Tree*/
void insert(fptree_node* &root,const vector<int> &items,int i);
void connect_next_link(fptree_node* root,Header *head);
ostream& operator<<(ostream&,const fptree_node&);
void level_print_fptree(fptree_node *root);

/* general utils*/
void generate_fptree(FileHandler* fptr);


Header::Header(const int nitems){
    this->total_n = nitems;
    this->curr_n = 0;

    items = new header_node*[nitems];
    item_pos = new int[nitems+1];

    for(int i=0;i<nitems;i++)
            this->items[i] = NULL;

}

void Header::insert(const vector<pair<int,int>> &item_count){
    assert(item_count.size()<=this->total_n);

    for(const pair<int,int> &count:item_count){
        items[curr_n] = new header_node();
        items[curr_n]->item_count = count.second;
        items[curr_n]->item_id = count.first;
        items[curr_n]->item_ptr = NULL;

        item_pos[count.first] = curr_n;

        curr_n++;
    }
}

header_node* Header::get_node(const int item_id){
    int pos = this->item_pos[item_id];

    return this->items[pos];
}

void Header::print(){
    for(int i=0;i<this->curr_n;i++){
        cout<<this->items[i]->item_id<<" : "<<this->items[i]->item_count<<" ---> ";
        fptree_node *curr = this->items[i]->item_ptr;
        while(curr!=NULL){
            cout<<*curr<<"--";
            curr = curr->next;
        }
        cout<<"|";
        cout<<endl;
    }
}


bool cmp_pair_des(const pair<int,int> &a,const pair<int,int> &b){
    //sort in decreasing order
    return a.second>=b.second;
}

void insert(fptree_node* &root,const vector<int> &items,int i=0){
    root->item_count += 1;
    if(i>=items.size())
        return;
    else{
        //check if children with current item_id exists
        int curr_item_id = items[i];
        int c_pos = 0;
        for(c_pos=0;c_pos<root->children.size();c_pos++){
            if(root->children[c_pos]->item_id == curr_item_id)
                break;
        }

        if(c_pos<root->children.size()){
            //item exists
            insert(root->children[c_pos],items,i+1);
        }else{
            //item doesn't exist, create a new branch
            fptree_node *temp = new fptree_node();
            temp->item_id = items[i];
            temp->item_count = 0;
            temp->parent = root;
            temp->next = NULL;

            root->children.push_back(temp);
            insert(temp,items,i+1);
        }
    }
}


/*
To connect the nodes, start with the last node for a item_id (go to the right first),
store this in head and connect the next node with item_id to this node\
Then replace the head with current node
*/
void connect_next_link(fptree_node* root,Header *head){
    for(int i=root->children.size()-1;i>=0;i--){
        int curr_item_id = root->children[i]->item_id;

        header_node * curr_head = head->get_node(curr_item_id);
        
        if(curr_head->item_ptr==NULL){
            //this is the last node
            curr_head->item_ptr = root->children[i];
            root->children[i]->next = NULL;
        }else{
            //connect the current node with last seen node
            root->children[i]->next = curr_head->item_ptr;

            //replace the head node
            curr_head->item_ptr = root->children[i];
        }

        connect_next_link(root->children[i],head);
    }
}


ostream& operator<<(ostream& stream,const fptree_node& s){
    stream<<"[ "<<s.item_id<<" : "<<s.item_count<<" ]";
    return stream;
}


/*
Print the tree level wise
*/
void level_print_fptree(fptree_node *root){
    queue<fptree_node *> Q;
    Q.push(root);
    Q.push(NULL);
    while(Q.front()!=NULL){
        while(Q.front()!=NULL){
            fptree_node* curr = Q.front();
            cout<<(*curr)<<" ";
            for(fptree_node* child:curr->children){
                Q.push(child);
            }
            Q.pop();
        }
        cout<<endl;
        Q.pop();
        Q.push(NULL);
    }
}


void generate_fptree(FileHandler* fptr){
    

    vector<pair<int,int>> item_count;
    unordered_map<int,int> item_count_pos;
    Header * head;
    fptree_node *root = new fptree_node();
    root->item_count = 0;
    root->item_id = -1;

    //get 1 itemset
    //read data in chunk to get 1-itemset
    int curr_pass = fptr->get_npasses();
    while(fptr->get_npasses()==curr_pass){
        Dataframe *df = new Dataframe;

        fptr->get_data_chunk(df,CHUNK_SIZE);

        //get the 1 itemset count of current partition
        vector<pair<int,int>> curr_partition_count = df->value_counts();

        //append current count to total count
        for(pair<int,int> &ic : curr_partition_count){
            unordered_map<int,int>::iterator it = item_count_pos.find(ic.first);
            if(it!=item_count_pos.end()){
                //item exits
                //increment the count
                item_count[it->second].second += ic.second;
            }else{
                //item doesn't exits
                item_count_pos[ic.first] = item_count.size();
                item_count.push_back(ic);
            }
        }

        delete df;
    }

    //item_count contains 1 itemset
    //sort in descending order
    sort(item_count.begin(),item_count.begin()+item_count.size(),cmp_pair_des);

    //initialize Header
    head = new Header(item_count.size());
    head->insert(item_count);

    //read data in chunk for creating the FP Tree
    curr_pass = fptr->get_npasses();
    while(fptr->get_npasses()==curr_pass){
        Dataframe *df = new Dataframe;

        fptr->get_data_chunk(df,CHUNK_SIZE);

        vector<string> columns = df->columns();

        for(string &col:columns){
            const vector<int>  curr_transaction = (*df)[col].get_data();

            vector<pair<int,int>> curr_transaction_count;
            for(const int item_id:curr_transaction){
                header_node *count_node = head->get_node(item_id);
                curr_transaction_count.push_back({item_id,count_node->item_count});
            }            

            //sort current transaction
            sort(curr_transaction_count.begin(),curr_transaction_count.begin()+curr_transaction_count.size(),cmp_pair_des);

            //store the items (item_ids)
            vector<int> items;
            items.reserve(curr_transaction_count.size());
            for(pair<int,int> &ic:curr_transaction_count){
                items.push_back(ic.first);
            }

            //insert current transaction into tree
            insert(root,items);
        }
        delete df;
    }


    //connect the next link
    connect_next_link(root,head);

    //print FP Tree
    cout<<"-------FP Tree----------"<<endl;
    level_print_fptree(root);

    //print Header
    cout<<"-------Header----------"<<endl;
    head->print();

}
//----------------FP Tree ENDS----------------

int main(int argc,char* argv[]){
    assert(argc>=2);

    FileHandler *fptr;

    fptr = new FileHandler();
    fptr->open_file(argv[1]);


    generate_fptree(fptr);
    return 0;
}