#!/usr/bin/python

import MySQLdb
"""db.echo = False  # Try changing this to True and see what happens

metadata = BoundMetaData(db)"""

class Connection(object):
    def __init__(self, dsn, user, password):
        self.dsn = dsn
        self.user = user
        self.password = password

        #QUESTION: can the stuff below be much cleaner?

        #dsn ==> mysql:dbname=testdb;host=localhost

        dsn_split = dsn.split(":") #['mysql', 'dbname=testdb;host=localhost']
        db_type = dsn_split[0] #mysql
        
        db_host = dsn_split[1].split(";") #['dbname=testdb', 'host=localhost']
        dbname = db_host[0].split("=")[1] #testdb
        hostname = db_host[1].split("=")[1] #localhost

        self.connection = MySQLdb.connect(hostname,user,password,dbname)
        self.cursor = self.connection.cursor()


class Table(object):
    def __init__(self, name, dbh, *args):
        self.name = name
        self.dbh = dbh
        self.columns = args

        for idx, val in enumerate(args):
            val.id = idx 

    def __getitem__(self, columns):
        #QUESTION: is it a bad thing that i'm resetting the value for columns here...or at all? this is the case for when selecting certain columns is desired.
        #resetting columns on table to correspond with the columns that were selected in select statement
        c = []
        
        for column in self.columns:
            try:
                column.id = list(columns).index(column.name)
                c.append(column)
            except:
                pass #print "will it keep going?"

        self.columns = tuple(c) 

        return columns

        """self.c = Columns() <---QUESTION: should i do something like this and then in the "Columns" class define a "__getitem__ method"
        plus in the __init__ method do the for loop below and also assign args to self.columns. would it be ridiculous to access columns
        something like self.columns.columns or self.c.columns? Also, is there any way to perform a __getitem__ on a list that doesn't
        belong to a class?
        """

    def create(self):
        primary_key = ""
        columns =""
        comma = ""

        for idx, arg in enumerate(self.columns):
            if idx!=0:
                comma = ", "

            if(arg.primary_key):
                #primary_key =" primary key, "
                columns += comma+arg.name+" "+arg.type+" primary key auto_increment" #TODO: be careful with this auto_increment, don't assume that that's what's wanted
            else:
                #primary_key =", "
                columns += comma+arg.name+" "+arg.type
                
            #columns += arg.name+" "+arg.type+primary_key

        sql = "create table "+self.name+"("+columns+");"
        
        # prepare a cursor object using cursor() method
        cursor = self.dbh.cursor
        try:
            cursor.execute(sql)
        except:
            self.dbh.connection.rollback()

        # disconnect from server
        #self.dbh.connection.close()


    def insert(self, *args):

        for arg in args:
            columns=[]
            values=[]
            sql=""
            
            #if isinstance(arg, dict): #multiple inserts TODO: remove this line. there will be only one way to insert into table
            for key, val in arg.items():
                if isinstance(val, str):
                    val = "'"+val+"'" #in sql, string values must be wrapped in quotes
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

        """sql = "select * from "+self.name+";"""

        cursor = self.dbh.cursor

        try:
            #cursor.execute(sql)
            results = Results(cursor.fetchone(), self.columns)
        except:
            self.dbh.connection.rollback()

        return results

    def fetch(self):

        results = ""
        result_objects = []

        #sql = "select * from "+self.name+";"

        cursor = self.dbh.cursor

        try:
            #cursor.execute(sql)
            results = cursor.fetchall()

            for result in results:
                result_object = Results(result, self.columns)
                result_objects.append(result_object)

        except:
            self.dbh.connection.rollback()

        
        return tuple(result_objects)

    """def select(self, *args):     
        if len(args)>0:
            print "args > 0"
        else:
            #no arguments passed, fetch all
            return self.select_all()"""

    def select(self, *args):
        if len(args) > 0: #i think this should be equal to one i.e. no where clause
            columns = []
            
            #for column in args:
            #   columns.append(column)  

            sql = "select "+','.join(args[0])+" from "+self.name+";"

        else:
            sql = "select * from "+self.name+";"
 
        cursor = self.dbh.cursor

        try:
            cursor.execute(sql)
        except:
            self.dbh.connection.rollback()

class Column(object):
    def __init__(self, name, data_type, primary_key=False):
        self.type = data_type.val
        self.name = name
        self.primary_key = primary_key

"""class Columns(object):
    def __init__(self, *args):
        self.args = args

    def __getitem__(self, x):
        print x"""


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
        for column in self.columns:
            if column.name == val:
                return column.id


class Integer(object):
    def __init__(self):
        self.val = "int"

class String(object):
    def __init__(self, val=''):
        self.val = "varchar("+str(val)+")"

dbh = Connection("mysql:dbname=testdb;host=localhost", "root", "mysql1")

users = Table('users', dbh,
    Column('user_id', Integer(), primary_key=True),
    Column('name', String(40)),
    Column('age', Integer()),
    Column('password', String(200))
)

#users.create()
#users.insert(name='Mary', age='30', password='secret')
#users.insert({'name': 'Mary', 'age':22, 'password':'guessit'})
"""users.insert(    {'name': 'Mary', 'age':22, 'password':'guessit'},
                {'name': 'Susan', 'age': 57},
                {'name': 'Carl', 'age': 33})"""

users.select(users["name", "age"])
row = users.fetchone()
print row[1]

"""row = users.fetchone()
print row["name"]"""

"""users.select(users["name", "age"])
rows = users.fetch()

for r in rows:
    print r["name"], 'is', r["age"], 'years old'"""


"""TODO:
-maybe add functionality that echoes the sql being executed
db.echo = True  # We want to see the SQL we're creating
-maybe add functionality that autoload table if it already exists 
basic:
-where
-and/or
-order by
-update?
-delete?

advanced:
-in
-join
-innerjoin
-leftjoin
-rightjoin
-fulljoin

functions:
-groupby
-count
-sum
"""



