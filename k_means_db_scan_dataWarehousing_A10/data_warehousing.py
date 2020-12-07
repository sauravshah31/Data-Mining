import sqlite3 


data_dir = "./data/"

conn = None
SQL = None
try:
    conn = sqlite3.connect(data_dir+"datacube.db")
    SQL = conn.cursor()
except:
    print("Unable to connect to database")
    exit(-1)


"""
DATA CUBE (for some school database) [Logical representation]
----------------------------------------------------------------------------------------
                                     SCHOOL RECORD
----------------------------------------------------------------------------------------
             Student        ||          Faculty      ||       Courses                       <- Dimension
----------------------------------------------------------------------------------------
ID | Name  |CourseId| grade || ID | Name | CourseId  || CourseId | Name                     <- Attributes
----------------------------------------------------------------------------------------
S1 | Hary  | C1     | 9     || F1 | SK   | C1        || C1       | Science
S1 | Hary  | C2     | 10    || F2 | TK   | C2        || C2       | History
S1 | Hary  | C3     | 8     || F1 | SK   | C3        || C3       | Fine Arts
S2 | Alica | C1     | 10    ||                       ||
S2 | Alica | C2     | 8     ||                       ||
S2 | Alica | C3     | 9     ||                       ||
----------------------------------------------------------------------------------------
"""



def create_table(tname,columns):
    global SQL
    sql_query = "CREATE TABLE "
    sql_query += tname
    sql_query += "("
    for column in columns:
        sql_query += column["name"] +" ";
        for constraint in column["constraints"]:
            sql_query += constraint
            sql_query += " "
        sql_query += ","
        
    sql_query = sql_query[:-1]
    sql_query += ");"

    print(f"EXECUTING QUERY\n{sql_query}")
    try:
        SQL.execute(sql_query)
        conn.commit()
    except:
        print("TABLE EXISTS")
    


def insert_into_table(tname,columns,values):
    global SQL
    sql_query = "INSERT INTO "
    sql_query += tname
    sql_query += "("
    for column in columns:
        sql_query += column
        sql_query += ","
    sql_query = sql_query[:-1]
    sql_query += ")"

    sql_query += " VALUES ("
    for value in values:
        if type(value) == str:
            sql_query += "'"+value+"'"
        else:
            sql_query += str(value)
        sql_query += ","
    sql_query = sql_query[:-1]
    sql_query += ");"

    print(f"EXECUTING QUERY\n{sql_query}")
    try:
        SQL.execute(sql_query)
        conn.commit()
    except:
        print("INSERTION FAILED")


def create_data_cube():
    global DIMENSION_TABLES

    """
    Data cube will contain 3 dimensions
        1) Student
        2) Faculty
        3) Course

    So (Student,Faculty,Course) gives the "Student" taking course "Course" taught by faculty "Faculty"

    This can be thought of as a 3D Array with size:
        (2,2,3)
        (Two unique students, Two unique faculty, Three unique courses)
    """

    """
    We use Star Schema to create the warehouse
            Fact Table
        --------------------------------
        Student(ID)
        Course(ID)
        Faculty(ID)
        ------------
        Grades <- obtained by student, given by faculty for a subject
    """

    print("CREATING TABLES")
    print("------------------------------------")
    #CREATE STUDENT TABLE
    tname = "student"
    columns = \
        [
            {
                "name":"ID",
                "constraints":["TEXT","PRIMARY KEY"]
            },
            {
                "name":"NAME",
                "constraints":["TEXT","NOT NULL"]
            },
        ]
    

    create_table(tname,columns)

    #CREATE FACULTY TABLE
    tname = "faculty"
    columns = \
        [
            {
                "name":"ID",
                "constraints":["TEXT","PRIMARY KEY"]
            },
            {
                "name":"NAME",
                "constraints":["TEXT","NOT NULL"]
            },
        ]
    

    create_table(tname,columns)

    #CREATE COURSE TABLE
    tname = "course"
    columns = \
        [
            {
                "name":"ID",
                "constraints":["TEXT","PRIMARY KEY"]
            },
            {
                "name":"NAME",
                "constraints":["TEXT","NOT NULL"]
            },
        ]
    

    create_table(tname,columns)


    #CREATE Fact TABLE
    tname = "fact"
    columns =  \
        [
            {
                "name":"SID",
                "constraints":["TEXT"]
            },
            {
                "name":"FID",
                "constraints":["TEXT"]
            },
            {
                "name":"CID",
                "constraints":["TEXT"]
            },
            {
                "name":"GRADE",
                "constraints":["INTEGER"]
            },
            {
                "name":"FOREIGN KEY",
                "constraints":["(SID) REFERENCES student(ID)"]
            },
            {
                "name":"FOREIGN KEY",
                "constraints":["(FID) REFERENCES course(ID)"]
            },
            {
                "name":"FOREIGN KEY",
                "constraints":["(CID) REFERENCES faculty(ID)"]
            }
        ]
    
    create_table(tname,columns)
    print()
    print("INSERTING DATA INTO TABLES")
    print("------------------------------------")

    #insert data into student table
    insert_into_table("student",["ID","NAME"],["S1","Hary"])
    insert_into_table("student",["ID","NAME"],["S2","Alica"])

    #insert data into faculty table
    insert_into_table("faculty",["ID","NAME"],["F1","SK"])
    insert_into_table("faculty",["ID","NAME"],["F2","TK"])

    #insert data into course table
    insert_into_table("course",["ID","NAME"],["C1","Science"])
    insert_into_table("course",["ID","NAME"],["C2","History"])
    insert_into_table("course",["ID","NAME"],["C3","Fine Arts"])

    #insert data into fact table
    insert_into_table("fact",["SID","FID","CID","GRADE"],["S1","F1","C1",9])
    insert_into_table("fact",["SID","FID","CID","GRADE"],["S1","F2","C2",10])
    insert_into_table("fact",["SID","FID","CID","GRADE"],["S1","F1","C3",8])

    insert_into_table("fact",["SID","FID","CID","GRADE"],["S2","F1","C1",10])
    insert_into_table("fact",["SID","FID","CID","GRADE"],["S2","F2","C2",8])
    insert_into_table("fact",["SID","FID","CID","GRADE"],["S2","F1","C3",9])


def print_datacube():
    print("*******STAR SCHEMA********")
    
    print("Fact Table")
    result = SQL.execute("SELECT * from fact;")
    print("SID\tFID\tCID\tGRADE")
    print('-'*40)
    for t in result:
        print(f"{t[0]}\t{t[1]}\t{t[2]}\t{t[3]}")

    print()
    print("Dimension Tables")
    print("Table : student")
    result = SQL.execute("SELECT * from student;")
    print("ID\tNAME")
    print('-'*40)
    for t in result:
        print(f"{t[0]}\t{t[1]}")
    print()
    
    print("Table : faculty")
    result = SQL.execute("SELECT * from faculty;")
    print("ID\tNAME")
    print('-'*40)
    for t in result:
        print(f"{t[0]}\t{t[1]}")
    print()
    
    print("Table : course")
    result = SQL.execute("SELECT * from course;")
    print("ID\tNAME")
    print('-'*40)
    for t in result:
        print(f"{t[0]}\t{t[1]}")
    print()


def rollup_operation():
    """
    We can now perform rollup opertion to reduce the dimension of data cube and get the summary for a particular dimension

    We can calculate the average of the grades of all the students in particular subject taught by particular faculty
    thereby reducing the dimension from 3D to 2D

    so our table becomes (Faculty,Course) -> gives the average grade of student in subject "Course" taught be teacher "Faculty"
    """

    
    sql_query = \
    """
    SELECT SID,NAME,avg(GRADE) AS overall_grade
    FROM 
    (SELECT SID,GRADE,NAME FROM fact
    INNER JOIN  student
    ON sid=id) 
    GROUP BY sid;
    """
    result = SQL.execute(sql_query)
    print("Average grade of students on all subjects")
    print("------------------------------------")
    print("SID\tNAME\tOVERALL_GRADE")
    for t in result:
        print(f"{t[0]}\t{t[1]}\t{t[2]}")
    

    sql_query = \
    """
    SELECT fid,faculty_name,cid,course_name,avg(grade) 
    FROM 
    (SELECT fid,cid,name AS faculty_name,grade 
    FROM faculty 
    INNER JOIN 
    (fact )
    ON fid=id) 
    INNER JOIN 
    (SELECT id, name AS course_name 
    FROM course) 
    ON cid=id 
    GROUP BY
    cid;
    """

    print()
    result = SQL.execute(sql_query)
    print("Average grade awarded by faculty")
    print("------------------------------------")
    print("FID\tFACULTY\tCID\tCOURSE\t\tAVERAGE_GRADE_AWARDED")
    for t in result:
        print(f"{t[0]}\t{t[1]}\t{t[2]}\t{t[3]}\t\t{t[4]}")


    sql_query = \
    """
    SELECT CID,NAME,avg(GRADE) AS average_grade
    FROM 
    (SELECT CID,GRADE,NAME FROM fact
    INNER JOIN  course
    ON cid=id) 
    GROUP BY cid;
    """
    print()
    result = SQL.execute(sql_query)
    print("Average grade (SUBJECT WISE)")
    print("------------------------------------")
    print("CID\tNAME\t\tAVERAGE_GRADE")
    for t in result:
        print(f"{t[0]}\t{t[1]}\t\t{t[2]}")
    


def start_datacube_opeartion():
    print("CREATING DATACUBE")
    print("-"*50)
    create_data_cube()

    print()
    print()
    print("DATACUBE CREATED IS")
    print("-"*50)
    print_datacube()

    print()
    print()
    print("DATACUBE (ROLLUP)")
    print("-"*50)
    rollup_operation()

if __name__ == "__main__":
    start_datacube_opeartion()

conn.close()

