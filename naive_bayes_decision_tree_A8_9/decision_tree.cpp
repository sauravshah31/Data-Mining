#include <iostream>
#include <vector>
#include <string>
#include <unordered_map> 
#include <unordered_set>
#include <cmath>
#include <cassert>
#include <queue>

#define BUFFER_SIZE 32768

using namespace std;


class Column{
public:
    bool is_numeric();
    void set_not_numeric();
    void insert(const double value);
    int insert(const string value);
    int get_label(const string value);
    void print();
    string get_string_value(double value);
    
public:
    vector<double> data;
protected:
    bool numeric=true;
    int curr_label = 0; 
    void* numeric_string_map=NULL; 
};

class Dataset{
public:

    vector<Column> data;
    void insert_columns(vector<string> column_names,vector<string> data_types);
    void insert(vector<string> tuple);
    const Column operator[](const string column_name);
    void print();

protected:
    vector<string> column_names;
    unordered_map<string,int> column_index;
};

void read_data_csv(char *file_name,vector<string> data_types,Dataset *output_container);
string strip_tspace(const string str);

typedef struct node{
    string feature;
    vector<node *> children;
    vector<double> label_value;
    int class_label;
}node;


class DecisionTreeClassifier:public Dataset{
public:
    void set_output_column(const string colname);
    vector<int> count(const string colname,double value,vector<int> rows);
    vector<int> count(const string colname,string value,vector<int> rows);
    vector<pair<int,vector<int>>> unique(const string colname);
    vector<pair<int,vector<int> > > unique(const string colname,vector<int> rows);
    void print_tree();

    void train();
    vector<int> predict(Dataset *input);
private:
    void feature_selection_entropy();
    node* train_util(vector<int> rows,vector<string> features);

    vector<int> count_util(int index,double value);
    vector<int> count_util(int index,double value,vector<int> rows);
private:
    node * T=NULL;
    string output_col_name;
    vector<string> prediction_class;
};

void DecisionTreeClassifier::set_output_column(const string colname){
    this->output_col_name = colname;
    vector<pair<int,vector<int>>> labels =  this->unique(colname);
    this->prediction_class.clear();
    this->prediction_class.reserve(labels.size());
    unordered_map<string,int>::iterator pos = Dataset::column_index.find(colname);
    int index;
    int n=0;
    assert(pos != Dataset::column_index.end());

    index = pos->second;
    cout<<index<<endl;
    for(int i=0;i<labels.size();i++){
        string val = Dataset::data[index].get_string_value(i);
        this->prediction_class.push_back(val);
    }
}


vector<int> DecisionTreeClassifier::count(const string colname,double value,vector<int> rows){
    unordered_map<string,int>::iterator pos = Dataset::column_index.find(colname);
    int index;
    assert(pos != Dataset::column_index.end());

    index = pos->second;
    return this->count_util(index,value,rows);    
}

vector<int> DecisionTreeClassifier::count(const string colname,string value,vector<int> rows){
    unordered_map<string,int>::iterator pos = Dataset::column_index.find(colname);
    int index;
    assert(pos != Dataset::column_index.end());

    index = pos->second;
    double numeric_value = Dataset::data[index].get_label(value);
    return this->count_util(index,numeric_value,rows);
}

vector<pair<int,vector<int>>> DecisionTreeClassifier::unique(const string colname){
    unordered_map<double,int> seen;
    vector<pair<int,vector<int>>> items;
    unordered_map<string,int>::iterator pos = Dataset::column_index.find(colname);
    int index;
    int n=0;
    assert(pos != Dataset::column_index.end());

    index = pos->second;
    int i=0;
    for(double val:Dataset::data[index].data){
        unordered_map<double,int>::iterator pos = seen.find(val);
        if(pos == seen.end()){
            items.emplace_back(val,vector<int>());
            items[n].second.push_back(i);
            seen[val] = n;
            n++;
        }else{
            items[pos->second].second.push_back(i);
        }
        i++;
    }
    return items;
}


vector<pair<int,vector<int> > > DecisionTreeClassifier::unique(const string colname,vector<int> rows){
    unordered_map<double,int> seen;
    vector<pair<int,vector<int>>> items;
    unordered_map<string,int>::iterator pos = Dataset::column_index.find(colname);
    int index;
    int n=0;
    double val;
    assert(pos != Dataset::column_index.end());

    index = pos->second;
    for(int i=0;i<rows.size();i++){
        val = Dataset::data[index].data[rows[i]];
        unordered_map<double,int>::iterator pos = seen.find(val);
        if(pos == seen.end()){
            items.emplace_back(val,vector<int>());
            items[n].second.push_back(rows[i]);
            seen[val] = n;
            n++;
        }else{
            items[pos->second].second.push_back(rows[i]);
        }
    }
    return items;
}

vector<int> DecisionTreeClassifier::count_util(int index,double value){
    vector<int> index_pos;
    for(int i=0;i<Dataset::data[index].data.size();i++){
        if(Dataset::data[index].data[i] == value)
            index_pos.push_back(i);
    }
    return index_pos;
}
vector<int> DecisionTreeClassifier::count_util(int index,double value,vector<int> rows){
    vector<int> index_pos;
    for(int i=0;i<rows.size();i++){
        if(Dataset::data[index].data[i] == value)
            index_pos.push_back(rows[i]);
    }
    return index_pos;
}

void DecisionTreeClassifier::train(){
    int nfeatures,nrows,output_feature_index;
    vector<string> features;
    vector<int> rows;
    nfeatures = Dataset::column_names.size()-1;
    nrows = Dataset::data[0].data.size();
    features.reserve(nfeatures);
    rows.reserve(nrows);
    output_feature_index = Dataset::column_index.find(this->output_col_name)->second;

    for(int i=0;i<nrows;i++)
        rows.push_back(i);
    
    for(int i=0;i<nfeatures;i++)
    {
        if(i!=output_feature_index){
            features.push_back(Dataset::column_names[i]);
        }
    }
    
    if(this->T != NULL){
        delete this->T;
        this->T = NULL;
    }
    this->T = this->train_util(rows,features);
}

node* DecisionTreeClassifier::train_util(vector<int> rows,vector<string> features){
    node *root = new node();
    //calculate no of unique classes in current dataset
    vector<pair<int,vector<int>>> classes = this->unique(this->output_col_name,rows);

    if(classes.size()==1){
        //leaf node, set the class
        root->class_label = classes[0].first;
    }else if(features.size()==0){
        //make this a leaf node and set the current class to the class majority of tuples belongs to
        int majority_count = classes[0].second.size();
        int majority_class = classes[0].first;
        for(int i=1;i<classes.size();i++){
            if(classes[i].second.size() > majority_count)
            {
                majority_count = classes[i].second.size();
                majority_class = classes[i].first;
            }
        }
        root->class_label = majority_class;
    }else{
        //choose the best feature
        //calculate the information gain
        
        double entropy_dataset,entorpy_feature,temp;
        double max_gain,curr_gain;
        int max_gain_feature;
        int ntuples;

        ntuples = rows.size();

        //entopy of dataset
        entropy_dataset = 0.0;
        for(int i=0;i<classes.size();i++){
            //pi*log(pi)
            temp = (classes[i].second.size()/(ntuples+0.0));
            entropy_dataset -= (temp*log2(temp));
        }

        //entropy after seleting each feature
        max_gain = -1;
        for(int i=0;i<features.size();i++){
            curr_gain = 0.0;
            vector<int> curr_count;
            int total = 0;
            curr_count.reserve(classes.size());
            for(int j=0;j<classes.size();j++){
                vector<pair<int,vector<int>>> curr_rows;
                curr_rows = this->unique(features[i],classes[j].second); 
                int count = 0;
                for(int k=0;k<curr_rows.size();k++)
                    count += curr_rows[k].second.size();
                total += count;
                curr_count.push_back(count);
            }
            for(int j=0;j<curr_count.size();j++){
                temp = curr_count[j]/(total+0.0);
                curr_gain -= (temp*log2(temp));
            }
            curr_gain = entropy_dataset - curr_gain;

            if(curr_gain > max_gain){
                max_gain = curr_gain;
                max_gain_feature = i;
            }
        }


        root->feature = features[max_gain_feature];
        vector<pair<int,vector<int>>> feature_branches = this->unique(features[max_gain_feature],rows);
        features.erase(features.begin()+max_gain_feature);

        root->children.reserve(feature_branches.size());
        root->label_value.reserve(feature_branches.size());

        for(int i=0;i<feature_branches.size();i++){
            node* child = this->train_util(feature_branches[i].second,features);
            root->children.push_back(child);
            root->label_value.push_back(feature_branches[i].first);
        }    
    }

    return root;
}

vector<int> DecisionTreeClassifier::predict(Dataset *input){
    vector<int> predictions;
    predictions.reserve(input->data[0].data.size());

    for(int i=0;i<input->data[0].data.size();i++){
        node *root = this->T;
        double data;
        while(root && root->children.size()!=0){
            data = (*input)[root->feature].data[i];
            int j=0;
            while(j<root->children.size() && root->label_value[j]!=data)
                j++;
            if(j>=root->children.size())
                break;
            root = root->children[j];
        }
        predictions.push_back(root->class_label);
    }
    return predictions;
}

void DecisionTreeClassifier::print_tree(){
    queue<node *> Q;
    Q.push(this->T);
    Q.push(NULL);
    while(Q.front()!=NULL){
        while(Q.front()!=NULL){
            if(Q.front()->children.size()!=0){
                cout<<Q.front()->feature<<" ";
            }else{
                cout<<Q.front()->class_label<<" ";
            }
            for(node *child:Q.front()->children){
                Q.push(child);
            }
            Q.pop();
        }
        Q.pop();
        Q.push(NULL);
        cout<<endl;
        cout<<Q.size()<<endl;
    }
}

//----------------------MAIN------------------

int main(){
    cout<<"----------Training---------"<<endl;
    DecisionTreeClassifier *classifier;
    classifier = new DecisionTreeClassifier();
    read_data_csv("inp.csv",{"non_numeric","non_numeric","non_numeric","non_numeric","non_numeric"},classifier);
    classifier->set_output_column("class:buy_computer");
    classifier->print();
    
    classifier->train();
    classifier->print_tree();
    cout<<endl;
    cout<<"---------------Testing----------"<<endl;
    cout<<"----------Train Set (inp.csv)----------"<<endl;
    Dataset *test_train = new Dataset();
    read_data_csv("inp.csv",{"non_numeric","non_numeric","non_numeric","non_numeric","non_numeric"},test_train);
    vector<int> predicted = classifier->predict(test_train);
    for(int i=0;i<predicted.size();i++){
        cout<<(predicted[i]==0?"no":"yes")<<endl;
    }

    cout<<"----------Test Set 1 (test1.csv)---------"<<endl;
    Dataset *test = new Dataset();
    read_data_csv("test1.csv",{"non_numeric","non_numeric","non_numeric","non_numeric","non_numeric"},test);
    test->print();
    predicted = classifier->predict(test);
    for(int i=0;i<predicted.size();i++){
        cout<<(predicted[i]==0?"no":"yes")<<endl;
    }

    cout<<"----------Test Set 2 (test2.csv)---------"<<endl;
    Dataset *test1 = new Dataset();
    read_data_csv("test2.csv",{"non_numeric","non_numeric","non_numeric","non_numeric"},test1);
    test1->print();
    predicted = classifier->predict(test1);
    for(int i=0;i<predicted.size();i++){
        cout<<(predicted[i]==0?"no":"yes")<<endl;
    }
}



bool Column::is_numeric(){
    return this->numeric;
}

void Column::set_not_numeric(){
    this->numeric = false;
    this->numeric_string_map = new unordered_map<string,int>();   
}

int Column::insert(const string value){
    assert(this->numeric==false && this->numeric_string_map!=NULL);
    int label;
    unordered_map<string,int> * mapping = (unordered_map<string,int> *)(this->numeric_string_map);
    unordered_map<string,int>::iterator pos = (mapping)->find(value);

    if(pos!= mapping->end()){
        //value already exists
        label = pos->second;
    }else{
        //value doesn't exists
        mapping->insert({value,this->curr_label});
        label = this->curr_label;
        this->curr_label++;
    }

    this->data.push_back(label);
    return label;
}

void Column::insert(double value){
    this->data.push_back(value);
}

int Column::get_label(const string value){
    assert(this->numeric_string_map != NULL);
    unordered_map<string,int> * mapping = (unordered_map<string,int> *)(this->numeric_string_map);
    unordered_map<string,int>::iterator pos = (mapping)->find(value);

    if(pos == mapping->end())
        return -1;

    return pos->second;    
}

void Column::print(){
    for(int i=0;i<data.size();i++){
        cout<<data[i]<<"\t";
    }
}
string Column::get_string_value(double value){
    if(this->numeric_string_map==NULL){
        return "";
    }
    for(auto i:*((unordered_map<string,double>*)numeric_string_map) ){
        if(abs(i.second-value)<0.001){
            return i.first;
        }
    }
    return "";
}

void Dataset::insert_columns(vector<string> column_names,vector<string> data_types){
    assert(column_names.size()==data_types.size());

    int n = column_names.size();
    this->column_names.reserve(n);

    for(int i=0;i<n;i++){
        this->column_names.push_back(column_names[i]);
        column_index[column_names[i]] = i;
        this->data.push_back(Column());

        if(data_types[i]=="numeric"){
        }else if(data_types[i]=="non_numeric"){
            this->data[i].set_not_numeric();
        }else{
            cerr<<"datatype "<<data_types[i]<<" not recognized\n possible values are :numeric, non_numeric";
            exit(1);
        }
    }

}

void Dataset::insert(vector<string> tuple){
    assert(tuple.size()==this->data.size());
    string value;
    for(int i=0;i<tuple.size();i++){
        value = tuple[i];
        if(this->data[i].is_numeric()){
            double temp = stod(value);
            this->data[i].insert(temp);
        }else{
            this->data[i].insert(value);
        }
    }
}

const Column Dataset::operator[](const string column_name){
    unordered_map<string,int>::iterator index = column_index.find(column_name);

    assert(index != this->column_index.end());

    return this->data[index->second];   
}
void Dataset::print(){
    for(int i=0;i<column_names.size();i++){
        cout<<column_names[i]<<"\t";
    }
    if(data.size()==0)
        return;
    cout<<endl;
    for(int i=0;i<data[0].data.size();i++){
        for(int j=0;j<data.size();j++){
            cout<<data[j].data[i]<<"\t";
        }
        cout<<endl;
    }
}


string strip_tspace(const string str){
    int i=0;
    while(i<str.size() && (str[i]==' ' || str[i]=='\t' || str[i]=='\n'))
        i++;
    
    int j=str.size()-1;
    while(j>i && (str[j]==' ' || str[j]=='\t' || str[j]=='\n'))
        j--;
    
    return str.substr(i,j+1);
}

void read_data_csv(char *file_name,vector<string> data_types,Dataset *output_container){
    char buffer[BUFFER_SIZE];
    size_t read_size;
    vector<string> values;
    string curr_value;
    FILE *fptr = fopen(file_name,"r");
    int n;

    if(fptr==NULL){
        cerr<<"Unable to open file"<<endl;
        exit(1);
    }

    curr_value.clear();
    while(fgets(buffer,BUFFER_SIZE,fptr)!=NULL){
        int i=0;
        while(buffer[i]!='\0' && buffer[i]!='\n'){
            if(buffer[i]==','){
                curr_value = strip_tspace(curr_value);
                values.push_back(curr_value);
                curr_value.clear();
            }else{
                curr_value += buffer[i];
            }
            i++;
        }
        if(buffer[i]=='\n')
            break;
    }
    curr_value = strip_tspace(curr_value);
    values.push_back(curr_value);
    curr_value.clear();
    
    n = values.size();
    output_container->insert_columns(values,data_types);
    values.clear();
    values.reserve(n);
    //values contains first line (column names)
    while(fgets(buffer,BUFFER_SIZE,fptr)!=NULL){
        int i=0;
        while(buffer[i]=='\n')
            i++;
        while(buffer[i]!='\0' && buffer[i]!='\n'){
            if(buffer[i]==','){
                curr_value = strip_tspace(curr_value);
                values.push_back(curr_value);
                curr_value.clear();
            }else{
                curr_value += buffer[i];
            }
            i++;
        }
        if(buffer[i]=='\n'){
            curr_value = strip_tspace(curr_value);
            values.push_back(curr_value);
            curr_value.clear();
            output_container->insert(values);
            values.clear();
            values.reserve(n);
        }
    }
    
    values.push_back(curr_value);
    output_container->insert(values);
    
    fclose(fptr);
}

