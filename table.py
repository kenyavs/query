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

        for idx, col in enumerate(columns):
            col.id = idx 


    def __getitem__(self, columns):
        return columns


    def create(self):
        primary_key = ""
        columns =""
        comma = ""

        for idx, column in enumerate(self.columns):
            if idx!=0:
                comma = ", "

            #TODO: rework sql statement to incorporate auto_increment, not_null and foreign key
            if column.auto_increment:
                auto_increment = "auto_increment"
            else:
                auto_increment = ""

            if column.not_null:
                not_null = "not_null"
            else:
                not_null = ""

            if column.foreign_key:
                foreign_key = "foreign key ("+column.foreign_key[1]+") references "+column.foreign_key[0]+"("+column.foreign_key[1]+")"
            else:
                foreign_key = ""

            if column.primary_key:
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
    def __init__(self, name, data_type, primary_key=False, auto_increment=False, not_null=False, foreign_key=[]):
        """TODO: user *args for dynamically setting attributes on Column object
            for prop in properties:
            print prop
            setattr(self, prop, prop)"""
        
        self.type = data_type.val
        self.name = name
        self.primary_key = primary_key
        self.auto_increment = auto_increment
        self.not_null = not_null
        self.foreign_key = foreign_key


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

    #def execute(self, sql, instance, attribute):
    #def execute(self, sql, data_class_instance, join_table):
    def execute(self, sql, data_class_instance, mapper):
        table = self.data_class.table
        cursor = table.dbh.cursor
        mapper_data_class = mapper.data_class
        join_table = mapper_data_class.table

        print "table name"
        print join_table.name

        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            join_table_instance_objects = []

            for result in results:
                #create new data_class object
                join_table_instance = mapper_data_class()
                current_state = {} #preserve the original state for updates

                for idx, column in enumerate(join_table.columns):
                    setattr(join_table_instance, column.name, result[idx])
                    current_state[column.name] = result[idx]
                    
                join_table_instance.current_state = current_state
                join_table_instance_objects.append(join_table_instance)

            print "about to set"
            print join_table.name
            print "this is the instance"
            print data_class_instance
            setattr(data_class_instance, join_table.name, join_table_instance_objects)
            print "what is the instance?"
            print data_class_instance
        except:
            table.dbh.connection.rollback()


        """try:
            cursor.execute(sql)
            result = cursor.fetchone()
            current_state = {} #preserve the original state for updates

            #create new data_class object
            instance = self.data_class()

            for idx, column in enumerate(self.fetch_columns):
                setattr(instance, column.name, result[idx])
                current_state[column.name] = result[idx]

            instance.current_state = current_state
        except:
            table.dbh.connection.rollback()"""

    #TODO: clean up function. This version of fetchone will only ever have where clause. i believe should always be selecting all columns
    # the length of clause should only ever be 1 and should always be a list.
    def fetchone(self, *clauses):
        table = self.data_class.table

        if isinstance(clauses[0], list): #is a list and therefore has a where clause
            sql = "select * from "+table.name+" where "+clauses[0][0]+" = '"+clauses[0][1]+"';"
            self.fetch_columns = table.columns
        else: #is not a list and therefore are columns
            print "Invalid arguments. Must be a list"
       
        #TODO: add fetch_columns to self.data_class vs self
        """if len(clauses) == 1:
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
            self.fetch_columns = table.columns"""
 
        cursor = table.dbh.cursor

        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            current_state = {} #preserve the original state for updates

            #create new data_class object
            instance = self.data_class()

            for idx, column in enumerate(self.fetch_columns):
                setattr(instance, column.name, result[idx])
                current_state[column.name] = result[idx]

            instance.current_state = current_state
        except:
            table.dbh.connection.rollback()


         #select * from users join emails on emails.user_id = users.user_id where users.name='Mary';
        if self.data_class.properties:
            mapper = self.data_class.properties.values()[0] #i.e. emailmapper, for now, only one item in dictionary
            join_table = mapper.table #i.e. emails table object


            for column in join_table.columns:
                if hasattr(column, "foreign_key") and column.foreign_key!=[]: #TODO: refactor line. should only check "hasattr. remove other condition"
                    match_column = column.foreign_key[1]
                    #TODO: STOP FOR LOOP, HOW IS THIS DONE?
            sql = "select "+join_table.name+".* from "+table.name+" join "+join_table.name+" on "+join_table.name+"."+match_column+"="+table.name+"."+match_column+" where "+clauses[0][0]+" = '"+clauses[0][1]+"';"
            #self.execute(sql, instance, join_table.name)
            self.execute(sql, instance, mapper)

        return instance


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
                value = self.formatValue(value)
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

            #rest columns and values
            columns = []
            values = []

            if "properties" in dir(data_class_instance): #data class has a relation i.e. User class relates to Email
                relation_table = data_class_instance.properties.keys()[0] #emails

                """print data_class_instance.table.columns
                for column in data_class_instance.table.columns:
                    print dir(column)
                    print "col name"
                    print column.name
                    if hasattr(column, "foreign_key") and column.foreign_key!=[]: #TODO: refactor line. should only check "hasattr. remove other condition"
                        print "has foreign_key"
                        match_column = column.foreign_key[1]
                        values.append(data_class_instance.match_column)
                        columns.append(match_column)
                    else:
                        pass"""            

                for email in data_class_instance.emails:
    
                    for column in email.table.columns:#relation_table.table.columns:

                        if hasattr(column, "foreign_key") and column.foreign_key!=[]: #TODO: refactor line. should only check "hasattr. remove other condition"
                            print "has foreign_key"
                            match_column = column.foreign_key[1]
                            #print data_class_instance.user_id
                            #print self.getMatchColumnIdFor(match_column)
                            values.append(self.getMatchColumnIdFor(match_column))
                            #print data_class_instance.user_id
                            columns.append(match_column)
                        else:
                            pass 

                        try:
                            value = getattr(email, column.name)
                            next_state[column.name] = value
                            value = self.formatValue(value)
                            columns.append(column.name)
                            values.append(value)
                        except:
                            pass

                    print "values"
                    print values

                    print "columns"
                    print columns
                    sql = "insert into "+relation_table+"("+','.join(columns)+") values("+','.join(values)+");"
                    print sql

                    cursor = table.dbh.cursor
                    print "insert"
                    print cursor
                    try:
                        cursor.execute(sql)
                        table.dbh.connection.commit()
                    except:
                        table.dbh.connection.rollback()

               

        except:
            table.dbh.connection.rollback()

    def update(self, data_class_instance):
        table = self.data_class.table
        updates = []
        next_state = {}

        for column in table.columns:
            value = getattr(data_class_instance, column.name)            
            next_state[column.name] = value
            value = self.formatValue(value)
            updates.append(column.name+"="+value)

        current_state = data_class_instance.current_state
        where_clauses = []

        for column in current_state:
            value = self.formatValue(current_state[column])
            where_clauses.append(column+"="+value)

        sql = "update "+table.name+" set "+','.join(updates)+" where "+' and '.join(where_clauses)+";"

        cursor = table.dbh.cursor
        try:
            cursor.execute(sql)
            table.dbh.connection.commit()
            data_class_instance.current_state = next_state
        except:
            table.dbh.connection.rollback()

    def formatValue(self, value):
        if isinstance(value, str):
            value = "'"+value+"'"
        else:
            value = str(value)

        return value

    def getMatchColumnIdFor(self, match_column):
        print "in getMatchColumnIdFor"
        print match_column
        table = self.data_class.table
        sql = "select max("+match_column+") from "+table.name+";"
        
        cursor = table.dbh.cursor

        try:
            cursor.execute(sql)
            print "here"
            result = cursor.fetchall()
            print "here now"
            print result
            for r in result:
                print "in for"
                user_id = r[0]
                user_id = self.formatValue(user_id)
            print user_id
        except:
            self.dbh.connection.rollback()

        return user_id

class Mapper(object):
    def __init__(self, data_class='', table='', properties=''):
        data_class.table = table
        data_class.properties = properties
        self.table = table
        self.properties = properties
        self.data_class = data_class


class User(object):
    pass

class Email(object):
    """def __init__(self, address):
        self.address = address"""

dbh = Connection("mysql:dbname=testdb;host=localhost", "root", "mysql1")

users = Table('users', dbh,
    #Column('user_id', Integer(), auto_increment=True, not_null=True, primary_key=True),
    Column('user_id', Integer(), primary_key=True),
    Column('name', String(40)),
    Column('age', Integer()),
    Column('password', String(200))
)

emails = Table('emails', dbh,
    Column('id', Integer(), primary_key=True),
    Column('address', String(50)),
    Column('user_id', Integer(), foreign_key=['users', 'user_id']))

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


"""usermapper = Mapper(User, users)
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


mary.password = 'guessit'
query.save(mary)"""

emailmapper = Mapper(Email, emails)
usermapper = Mapper(User, users, {'emails':emailmapper})
query = Query(User)
mary = query.fetchone([users['name'], 'Mary'])

print mary.age
for email in mary.emails:
    print email.address
#mary.age = mary.age+1
#print mary.age
query.save(mary)


print "Harry!"
harry = User()#User(name='Harry', age=47)
harry.name = 'Harry'
harry.age = 47
em1 = Email()#'harry@nowhere.com')
em1.address = 'harry@nowhere.com'
em2 = Email()#'harry@example.org')
em2.address = 'harry@example.org'
#em1.user = harry  # Properly updates the harry.emails property
#print harry.emails
harry.emails = []
harry.emails.append(em2)  # Properly sets em2.user
harry.emails.append(em1)
# Let's prove that harry.emails and em2.user were properly set
#print em2.user
#print harry.emails

query.save(harry)



"""TODO:
-do existing TODO's
-implement update for regular SQL render portion
-refactor fetchone for Query class
-does it make sense to limit one or fetchone
-how would fetching all results work?
-general clean up and commenting
-look into one to many
-look into many to many
-look into join
-look into including other SQL drivers
-brainstorm on next project, possibly using orm
-implement autoloading table, if it already exists
""" 