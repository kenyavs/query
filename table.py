#!/usr/bin/python

import MySQLdb

class Connection(object):
    def __init__(self, dsn, user, password):
        self.dsn = dsn
        self.user = user
        self.password = password

        #dsn ==> mysql:dbname=testdb;host=localhost

        dsn_split = dsn.split(":") #['mysql', 'dbname=testdb;host=localhost']
        db_type = dsn_split[0] #mysql
        
        db_host = dsn_split[1].split(";") #['dbname=testdb', 'host=localhost']
        dbname = db_host[0].split("=")[1] #testdb
        hostname = db_host[1].split("=")[1] #localhost

        self.connection = MySQLdb.connect(hostname,user,password,dbname)
        self.cursor = self.connection.cursor()

class Table(object):
    def __init__(self, name, dbh, *columns):
        self.name = name
        self.dbh = dbh
        self.columns = columns

        for idx, val in enumerate(columns):
            val.id = idx 


    def __getitem__(self, columns):
        return columns


    def create(self):
        primary_key = ""
        columns =""
        comma = ""

        for idx, column in enumerate(self.columns):
            if idx!=0:
                comma = ", "

            if(arg.primary_key):
                columns += comma+column.name+" "+column.type+" primary key auto_increment" #TODO: be careful with this auto_increment, don't assume that that's what's wanted
            else:
                columns += comma+column.name+" "+column.type
                
        sql = "create table "+self.name+"("+columns+");"
        
        # prepare a cursor object using cursor() method
        cursor = self.dbh.cursor
        try:
            cursor.execute(sql)
        except:
            self.dbh.connection.rollback()


    def insert(self, *cols):

        for col in cols:
            columns=[]
            values=[]
            sql=""
            
            for key, val in col.items():
                if isinstance(val, str):
                    val = "'"+val+"'"
                else:
                    val = str(val)

                columns.append(str(key))  
                values.append(val)

            sql = "insert into "+self.name+"("+','.join(columns)+") values ("+','.join(values)+");"

            cursor = self.dbh.cursor
            try:
                cursor.execute(sql)
                self.dbh.connection.commit()
            except:
                self.dbh.connection.rollback()


    def fetchone(self):

        results =""

        cursor = self.dbh.cursor

        try:
            results = Results(cursor.fetchone(), self.fetch_columns)
        except:
            self.dbh.connection.rollback()

        return results


    def fetch(self):
        results = ""
        result_objects = []

        cursor = self.dbh.cursor

        try:
            results = cursor.fetchall()

            for result in results:
                result_object = Results(result, self.fetch_columns)
                result_objects.append(result_object)
        except:
            self.dbh.connection.rollback()

        return tuple(result_objects)


    def delete(self, *args):
        if len(args) > 1:
            sql = "delete from "+self.name+" where "+args[0]+" = "+"'"+args[1]+"';"
        elif len(args) == 0:
            sql = "delete from "+self.name+";"

        cursor = self.dbh.cursor

        try:
            cursor.execute(sql)
            self.dbh.connection.commit()
        except:
            self.dbh.connection.rollback() 


    def select(self, *clauses):
        if len(clauses) == 1:
            if isinstance(clauses[0], list): #is a list and therefore has a where clause
                sql = "select * from "+self.name+" where "+clauses[0][0]+" = '"+clauses[0][1]+"';"
                self.fetch_columns = self.columns
            else: #is not a list and therefore are columns
                if isinstance(clauses[0], tuple):
                    sql = "select "+','.join(clauses[0])+" from "+self.name+";"
                    self.assignFetchColumns(clauses[0])
                else:
                    sql = "select "+clauses[0]+" from "+self.name+";"
                    self.assignFetchColumns(clauses[0])
        elif len(clauses) == 2:
            if isinstance(clauses[0], tuple): #multiple columns
                sql = "select "+','.join(clauses[0])+" from "+self.name+" where "+clauses[1][0]+" = '"+clauses[1][1]+"';"
                self.assignFetchColumns(clauses[0])
            else:
                sql = "select "+clauses[0]+" from "+self.name+" where "+clauses[1][0]+" = '"+clauses[1][1]+"';"
                self.assignFetchColumns(clauses[0])
        else:
            sql = "select * from "+self.name+";"
            print sql
            self.fetch_columns = self.columns
            print self.columns
 
        cursor = self.dbh.cursor

        try:
            cursor.execute(sql)
        except:
            self.dbh.connection.rollback()


    def assignFetchColumns(self, fetch_columns):        
        c = []

        for column in self.columns:
            try:
                if isinstance(fetch_columns, tuple):
                    column.id = list(fetch_columns).index(column.name)#get the index of the current column's name
                else: #not a list...a singular value and therefore first index, 0
                    if fetch_columns == column.name:
                        column.id = 0

                c.append(column)
            except:
                pass

        self.fetch_columns = tuple(c)


class Column(object):
    def __init__(self, name, data_type, primary_key=False):
        self.type = data_type.val
        self.name = name
        self.primary_key = primary_key


class Results(object):
    def __init__(self, result, columns):
        self.result = result
        self.columns = columns


    def __getitem__(self, x):
        try:
            if isinstance(x, str): #TODO: apparently it's more pythonic to do a try catch than to use isinstance :/
                col_id = self.getColumnIdFor(x)
                return self.result[col_id]
            else:
                return self.result[x]
        except:
            return None


    def getColumnIdFor(self, val):
        #iterate through columns. if the column name equals the string/name passed in, return the column's id value
        for column in self.columns:
            if column.name == val:
                return column.id


class Integer(object):
    def __init__(self):
        self.val = "int"


class String(object):
    def __init__(self, val=''):
        self.val = "varchar("+str(val)+")"


class Query(object):
    def __init__(self, data_class):
        self.data_class = data_class


    def fetchone(self, *clauses):
        table = self.data_class.table
       
        #TODO: add fetch_columns to self.data_class vs self
        if len(clauses) == 1:
            if isinstance(clauses[0], list): #is a list and therefore has a where clause
                sql = "select * from "+table.name+" where "+clauses[0][0]+" = '"+clauses[0][1]+"';"
                self.fetch_columns = table.columns
            else: #is not a list and therefore are columns
                if isinstance(clauses[0], tuple):
                    sql = "select "+','.join(clauses[0])+" from "+table.name+";"
                    self.fetch_columns = clauses[0]
                else:
                    sql = "select "+clauses[0]+" from "+table.name+";"
                    self.fetch_columns = clauses[0]
        elif len(clauses) == 2:
            if isinstance(clauses[0], tuple): #multiple columns
                sql = "select "+','.join(clauses[0])+" from "+table.name+" where "+clauses[1][0]+" = '"+clauses[1][1]+"';"
                self.fetch_columns = clauses[0]
            else:
                sql = "select "+clauses[0]+" from "+table.name+" where "+clauses[1][0]+" = '"+clauses[1][1]+"';"
                self.fetch_columns = clauses[0]
        else:
            sql = "select * from "+table.name+";"
            self.fetch_columns = table.columns
 
        cursor = table.dbh.cursor

        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            current_state = {} #preserve the original state for updates

            #create new data_class object
            instance = self.data_class()

            for idx, column in enumerate(self.fetch_columns):
                #setattr(self.data_class, column.name, result[idx])
                setattr(instance, column.name, result[idx])
                current_state[column.name] = result[idx]

            #self.data_class.current_state = current_state
            instance.current_state = current_state
        except:
            table.dbh.connection.rollback()

        return instance#self.data_class


    def save(self, data_class_instance=''):
        try:
            getattr(data_class_instance, "current_state")
            self.update(data_class_instance)
        except:
            self.insert(data_class_instance)

    def insert(self, data_class_instance):
        table = self.data_class.table
        values = []
        columns = []
        next_state = {}

        for column in table.columns:
            try:
                value = getattr(data_class_instance, column.name)
                next_state[column.name] = value
                
                if isinstance(value, str):
                    value = "'"+value+"'"
                else:
                    value = str(value)

                columns.append(column.name)
                values.append(value)
            except:
                pass #no such property defined yet

        sql = "insert into "+table.name+"("+','.join(columns)+") values("+','.join(values)+");"

        cursor = table.dbh.cursor
        try:
            cursor.execute(sql)
            table.dbh.connection.commit()
            data_class_instance.current_state = next_state
        except:
            table.dbh.connection.rollback()

    def update(self, data_class_instance):
        table = self.data_class.table
        updates = []
        next_state = {}

        #TODO: turn the following for loops into a function.
        for column in table.columns:
            #value = getattr(self.data_class, column.name)
            value = getattr(data_class_instance, column.name)
            print value
            
            next_state[column.name] = value
            if isinstance(value, str):
                value = "'"+value+"'"
            else:
                value = str(value)

            updates.append(column.name+"="+value)
        current_state = data_class_instance.current_state
        
        where_clauses = []

        for column in current_state:
            if isinstance(current_state[column], str):
                value = "'"+current_state[column]+"'"
            else:
                value = str(current_state[column])

            where_clauses.append(column+"="+value)

        sql = "update "+table.name+" set "+','.join(updates)+" where "+' and '.join(where_clauses)+";"
        print sql

        cursor = table.dbh.cursor
        try:
            cursor.execute(sql)
            table.dbh.connection.commit()
            data_class_instance.current_state = next_state
        except:
            table.dbh.connection.rollback()

class Mapper(object):
    def __init__(self, data_class, table):
        data_class.table = table


class User(object):
    pass



dbh = Connection("mysql:dbname=testdb;host=localhost", "root", "mysql1")

users = Table('users', dbh,
    Column('user_id', Integer(), primary_key=True),
    Column('name', String(40)),
    Column('age', Integer()),
    Column('password', String(200))
)

#users.create()
#users.insert({'name': 'Mary', 'age':22, 'password':'guessit'})
"""users.insert({'name': 'Mary', 'age':22, 'password':'guessit'},
                {'name': 'Susan', 'age': 57},
                {'name': 'Carl', 'age': 33})"""

#users.delete()

"""users.select()
rows = users.fetch()

for r in rows:
    print r[0], r["name"], r[2], r[3]"""

"""users.select([users['name'], 'Mary'])
row = users.fetchone()

print row
print row['user_id'], row['name'], row['age'], row['password']"""


Mapper(User, users)
query = Query(User)

mary = query.fetchone([users['name'], 'Mary'])
mary.age = mary.age+1

print mary.age

query.save(mary)

fred = User()
fred.name = "Fred"
fred.age = 57
fred.password = 'password'

print fred.password
query.save(fred)


mary.password = 'ambiguous'
query.save(mary)

