#!/usr/bin/python

import MySQLdb

#represents a connection to the database
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

#represents a table in a database
class Table(object):
    def __init__(self, name, dbh, *columns):
        self.name = name
        self.dbh = dbh
        self.columns = columns

        #attach id values to column objetc
        for idx, col in enumerate(columns):
            col.id = idx 

    #returns column ojects, tuple. double check edge case for a single column, ensure that it's in fact a tuple.
    def __getitem__(self, columns):
        return columns

    #creates a table for the database
    def create(self):
        primary_key = ""
        columns =""
        comma = ""

        for idx, column in enumerate(self.columns):
            if idx!=0:
                comma = ", "

            if column.primary_key:
                columns += comma+column.name+" "+column.type+" primary key auto_increment"
            else:
                columns += comma+column.name+" "+column.type
                
        sql = "create table "+self.name+"("+columns+");"
        
        # prepare a cursor object using cursor() method
        cursor = self.dbh.cursor
        try:
            cursor.execute(sql)
        except:
            self.dbh.connection.rollback()

    #inserts values into a database table
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

    #fetches a result set and returns one row at a time
    def fetchone(self):

        cursor = self.dbh.cursor

        try:
            return Results(cursor.fetchone(), self.fetch_columns)
        except:
            self.dbh.connection.rollback()

    #fetches and returns the entire result set
    def fetch(self):
        cursor = self.dbh.cursor

        try:
            results = cursor.fetchall()
            result_objects = [Results(result, self.fetch_columns) for result in results]
            return tuple(result_objects)
        except:
            self.dbh.connection.rollback()

    #delete an entry(entries) from a database table. in case of where, perhaps try chaining a list of tuples. i.e. [()]
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

    #select/read operation on a database table. TODOl think about breaking apart the select and where clauses...and removing the execution.
    def select(self, *clauses):
        if len(clauses) == 1: #only one argument is passed. either a where clause or columns for selection
            if isinstance(clauses[0], list): #is a list and therefore has a where clause
                sql = "select * from "+self.name+" where "+clauses[0][0]+" = '"+clauses[0][1]+"';"
                self.fetch_columns = self.columns
            else: #is not a list and therefore are columns
                if isinstance(clauses[0], tuple): #more than one column
                    sql = "select "+','.join(clauses[0])+" from "+self.name+";"
                    self.assignFetchColumns(clauses[0])
                else:
                    sql = "select "+clauses[0]+" from "+self.name+";" #only one column
                    self.assignFetchColumns(clauses[0])
        elif len(clauses) == 2: #has a where clause and columns for selection
            if isinstance(clauses[0], tuple): #multiple columns
                sql = "select "+','.join(clauses[0])+" from "+self.name+" where "+clauses[1][0]+" = '"+clauses[1][1]+"';"
                self.assignFetchColumns(clauses[0])
            else:
                sql = "select "+clauses[0]+" from "+self.name+" where "+clauses[1][0]+" = '"+clauses[1][1]+"';" #where clause and only one column
                self.assignFetchColumns(clauses[0])
        else:
            sql = "select * from "+self.name+";" #no where clause and all columns
            self.fetch_columns = self.columns
 
        cursor = self.dbh.cursor

        try:
            cursor.execute(sql)
        except:
            self.dbh.connection.rollback()

    #assigning fetch columns in order to keep the original list of columns in tact. columns to be fetched vs columns on a table may vary
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

#represents a column of a table
class Column(object):
    def __init__(self, name, data_type, primary_key=False, auto_increment=False, not_null=False, foreign_key=[]):
        
        self.type = data_type.val
        self.name = name
        self.primary_key = primary_key
        self.auto_increment = auto_increment
        self.not_null = not_null
        self.foreign_key = foreign_key

#represents a result set of a fetch
class Results(object):
    def __init__(self, result, columns):
        self.result = result
        self.columns = columns

    #allowing for accessing by column name or index
    def __getitem__(self, x):
        try:
            if isinstance(x, str):
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

#represents a string value for integer
class Integer(object):
    def __init__(self):
        self.val = "int"

#represents a string value for a string, a database representation of a string
class String(object):
    def __init__(self, val=''):
        self.val = "varchar("+str(val)+")"


#represents a query for the database, specifically handling data mapping
class Query(object):
    def __init__(self, data_class):
        self.data_class = data_class

    """executes and fetches all results of a given sql statement. arguments are the sql statement, the instance of the data class and the 
        mapper object that maps the data class to another data class
    """
    def execute(self, sql, data_class_instance, mapper):
        table = self.data_class.table
        cursor = table.dbh.cursor
        mapper_data_class = mapper.data_class 
        join_table = mapper_data_class.table #TODO: change name to mapper_data_class_table?

        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            join_table_instance_objects = []

            for result in results:
                #create new data_class object
                join_table_instance = mapper_data_class()
                current_state = {} #preserve the original state for updates

                """set the attributes on the newly created data class object with the results fetched, basically build the object dynamically
                    with the data from the data base
                """
                for idx, column in enumerate(join_table.columns):
                    setattr(join_table_instance, column.name, result[idx])
                    current_state[column.name] = result[idx]
                    
                join_table_instance.current_state = current_state
                join_table_instance_objects.append(join_table_instance)

            #set the property of the data class instance with the new object/objects
            setattr(data_class_instance, join_table.name, join_table_instance_objects)
        except:
            table.dbh.connection.rollback()


    # the length of clause should only ever be 1 and should always be a list.
    def fetchone(self, *clauses):
        table = self.data_class.table
        print table.name

        if isinstance(clauses[0], list): #is a list and therefore has a where clause
            sql = "select * from "+table.name+" where "+clauses[0][0]+" = '"+clauses[0][1]+"';"
            self.fetch_columns = table.columns
        else: #is not a list and therefore are columns
            print "Invalid arguments. Must be a list"
       
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

        if self.data_class.properties:
            mapper = self.data_class.properties.values()[0] #i.e. emailmapper, for now, only one item in dictionary
            join_table = mapper.table #i.e. emails table object

            for column in join_table.columns:
                if hasattr(column, "foreign_key") and column.foreign_key!=[]:
                    match_column = column.foreign_key[1]
            sql = "select "+join_table.name+".* from "+table.name+" join "+join_table.name+" on "+join_table.name+"."+match_column+"="+table.name+"."+match_column+" where "+clauses[0][0]+" = '"+clauses[0][1]+"';"
            self.execute(sql, instance, mapper)

        return instance

    #saves data for a specified object
    def save(self, data_class_instance=''):
        try:
            getattr(data_class_instance, "current_state")
            self.update(data_class_instance)
        except:
            self.insert(data_class_instance)

    #inserts data into database for a specified object
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

            #reset columns and values
            columns = []
            values = []

            if "properties" in dir(data_class_instance): #data class has a relation i.e. User class relates to Email
                relation_table = data_class_instance.properties.keys()[0] #emails  

                for email in data_class_instance.emails:
    
                    for column in email.table.columns:#relation_table.table.columns:

                        if hasattr(column, "foreign_key") and column.foreign_key!=[]:
                            values.append(self.getMatchColumnIdFor(match_column))
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

                    sql = "insert into "+relation_table+"("+','.join(columns)+") values("+','.join(values)+");"

                    cursor = table.dbh.cursor
                    
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
        table = self.data_class.table
        sql = "select max("+match_column+") from "+table.name+";"
        
        cursor = table.dbh.cursor

        try:
            cursor.execute(sql)
            result = cursor.fetchall()
    
            for r in result:
                user_id = r[0]
                user_id = self.formatValue(user_id)
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
