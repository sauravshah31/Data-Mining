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
    int get_col_index(string colname);

public:
    vector<string> column_names;
    unordered_map<string,int> column_index;
};

void read_data_csv(char *file_name,vector<string> data_types,Dataset *output_container);
string strip_tspace(const string str);


class BayesianClassifier:public Dataset{
public:
    void set_output_column(const string colname);
    vector<int> count(const string colname,double value,vector<int> rows);
    vector<int> count(const string colname,string value,vector<int> rows);
    vector<pair<int,vector<int>>> unique(const string colname);
    vector<pair<int,vector<int> > > unique(const string colname,vector<int> rows);
    void train();
    vector<int> predict(Dataset *input,int rowStart,int rowEnd);
    

private:
    void feature_selection_entropy();

    vector<int> count_util(int index,double value);
    vector<int> count_util(int index,double value,vector<int> rows);
private:
    double ***model=NULL;
    double *pclasses=NULL;
    int nfeatures=0,nclasses=0;
    string output_col_name;
    vector<string> prediction_class;
};



//---------------------MAIN--------------------------
int main(){
    cout<<"----------Training---------"<<endl;
    BayesianClassifier *classifier;
    classifier = new BayesianClassifier();
    read_data_csv("inp.csv",{"non_numeric","non_numeric","non_numeric","non_numeric","non_numeric"},classifier);
    classifier->set_output_column("class:buy_computer");
    classifier->print();
    classifier->train(); 
    cout<<endl;

    cout<<"---------------Testing----------"<<endl;
    cout<<"----------Train Set (inp.csv)----------"<<endl;
    Dataset *test_train = new Dataset();
    read_data_csv("test_naive_bayes.csv",{"non_numeric","non_numeric","non_numeric","non_numeric"},test_train);
    
    vector<int> predicted = classifier->predict(test_train,0,14);
    for(int i=0;i<predicted.size();i++){
        cout<<(predicted[i]==0?"no":"yes")<<endl;
    }
    

    cout<<"----------Testing 1 (test1.csv)---------"<<endl;
    Dataset *test = new Dataset();
    read_data_csv("test1.csv",{"non_numeric","non_numeric","non_numeric","non_numeric","non_numeric"},test);
    test->print();
    
    predicted = classifier->predict(test_train,14,17);
    for(int i=0;i<predicted.size();i++){
        cout<<(predicted[i]==0?"no":"yes")<<endl;
    }
    
    cout<<"----------Testing 2 (test2.csv)---------"<<endl;
    Dataset *test1 = new Dataset();
    read_data_csv("test2.csv",{"non_numeric","non_numeric","non_numeric","non_numeric"},test1);
    test1->print();
    
    predicted = classifier->predict(test_train,17,19);
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

int Dataset::get_col_index(string colname){
    unordered_map<string,int>::iterator pos = column_index.find(colname);
    int index;
    int n=0;
    assert(pos != Dataset::column_index.end());

    index = pos->second;
    return index;
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




vector<int> BayesianClassifier::count(const string colname,double value,vector<int> rows){
    unordered_map<string,int>::iterator pos = Dataset::column_index.find(colname);
    int index;
    assert(pos != Dataset::column_index.end());

    index = pos->second;
    return this->count_util(index,value,rows);    
}

vector<int> BayesianClassifier::count(const string colname,string value,vector<int> rows){
    unordered_map<string,int>::iterator pos = Dataset::column_index.find(colname);
    int index;
    assert(pos != Dataset::column_index.end());

    index = pos->second;
    double numeric_value = Dataset::data[index].get_label(value);
    return this->count_util(index,numeric_value,rows);
}

vector<pair<int,vector<int>>> BayesianClassifier::unique(const string colname){
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


vector<pair<int,vector<int> > > BayesianClassifier::unique(const string colname,vector<int> rows){
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


void BayesianClassifier::set_output_column(const string colname){
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


vector<int> BayesianClassifier::count_util(int index,double value){
    vector<int> index_pos;
    for(int i=0;i<Dataset::data[index].data.size();i++){
        if(Dataset::data[index].data[i] == value)
            index_pos.push_back(i);
    }
    return index_pos;
}
vector<int> BayesianClassifier::count_util(int index,double value,vector<int> rows){
    vector<int> index_pos;
    for(int i=0;i<rows.size();i++){
        if(Dataset::data[index].data[i] == value)
            index_pos.push_back(rows[i]);
    }
    return index_pos;
}

void BayesianClassifier::train(){
    vector<pair<int,vector<int>>> classes = this->unique(this->output_col_name);
    int nfeatures = Dataset::column_names.size()-1;
    int nclasses = classes.size();

    this->nfeatures = nfeatures;
    this->nclasses = nclasses;

    pclasses = new double[nclasses];
    int total_rows;

    total_rows = 0;
    for(int i=0;i<classes.size();i++){
        total_rows += classes[i].second.size();
    }

    for(int i=0;i<classes.size();i++){
        pclasses[i] = (classes[i].second.size()+0.0) / total_rows;
    }
    /*
    Model (nfeatures * nclasses * unique_values_for_that_feature)
    --------
                    yes     no
    ------------------------------
    age     youth |      |
            middle|      |
            senior|      |
    income        |      |
            ...
    student       |      |
            ...
    credit_rating |      |
            ...
    */
    this->model = new double**[nfeatures];
    for(int i=0;i<nfeatures;i++){
        this->model[i] = new double*[nclasses];
    }


    for(int i=0;i<nfeatures;i++){
        for(int j=0;j<nclasses;j++){
            model[i][j] = NULL;
        }
    }

    for(int i=0;i<this->nfeatures;i++){
        for(int j=0;j<this->nclasses;j++){
            //   feature i, class j
            //eg: age     , yes
            vector<pair<int,vector<int>>> curr_feature_unique = this->unique(Dataset::column_names[i],classes[j].second); 

            //model[i][j][0] will store no of unique classes for this feature given this classes
            model[i][j] = new double[curr_feature_unique.size()+1];
            model[i][j][0] = curr_feature_unique.size();

            //model[i][j][k] will store  P(class[j] | value_k_of_feature_i)
            for(int k=0;k<curr_feature_unique.size();k++){
                model[i][j][curr_feature_unique[k].first+1] = (curr_feature_unique[k].second.size()+0.0) / classes[j].second.size();
            }
        }
    }
}

vector<int> BayesianClassifier::predict(Dataset *input,int rowStart,int rowEnd){
    vector<int> predictions;
    predictions.reserve(rowEnd-rowStart+1);
    for(int i=rowStart;i<rowEnd;i++){
        double curr_max = -1;
        int max_index = -1;
        for(int j=0;j<this->nclasses;j++){
            double curr_prob = this->pclasses[j];
            for(int k=0;k<input->column_names.size();k++){
                int col_index = Dataset::get_col_index(input->column_names[k]);
                curr_prob *= this->model[col_index][j][(int)input->data[k].data[i]];
            }
            if(curr_prob>curr_max){
                curr_max=curr_prob;
                max_index = j;
            }
            
        }
        predictions.push_back(max_index);
    }
    return predictions;
}