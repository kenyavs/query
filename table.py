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

		self.db = MySQLdb.connect(hostname,user,password,dbname)
		self.cursor = self.db.cursor()


class Table(object):
	def __init__(self, name, db, *args):
		self.name = name
		self.db = db
		self.arguments = args


	def create(self):
		primary_key = ""
		columns =""
		comma = ""

		for idx, arg in enumerate(self.arguments):
			if idx!=0:
				comma = ", "

			if(arg.primary_key):
				#primary_key =" primary key, "
				columns += comma+arg.name+" "+arg.type+" primary key auto_increment"
			else:
				#primary_key =", "
				columns += comma+arg.name+" "+arg.type
				
			#columns += arg.name+" "+arg.type+primary_key

		sql = "create table "+self.name+"("+columns+");"

		# prepare a cursor object using cursor() method
		print "in create"
		cursor = self.db.cursor
		cursor.execute(sql)

		# disconnect from server
		#self.db.close() QUESTION: why wouldn't this be working?

#INSERT INTO EMPLOYEE(FIRST_NAME, LAST_NAME, AGE, SEX, INCOME) VALUES ('Mac', 'Mohan', 20, 'M', 2000)

	def insert(self, *args):

		for arg in args:
			columns=[]
			values=[]
			sql=""
			
			if isinstance(arg, dict): #multiple inserts TODO: remove this line. there will be only one way to insert into table
				for key, val in arg.items():
					if isinstance(val, str):
						val = "'"+val+"'" #in sql, string values must be wrapped in quotes
					else:
						val = str(val)

					columns.append(str(key))  
					values.append(val)

				sql = "insert into "+self.name+"("+','.join(columns)+") values ("+','.join(values)+");"
				cursor = self.db.cursor
				print "insert"
				print cursor
				cursor.execute(sql)
				#self.db.cursor.execute(sql)

class Column(object):
	def __init__(self, name, data_type, primary_key=False):
		#print data_type.val

		self.type = data_type.val
		self.name = name
		self.primary_key = primary_key

class Integer(object):
	def __init__(self):
		self.val = "int"

class String(object):
	def __init__(self, val=''):
		self.val = "varchar("+str(val)+")"

db = Connection("mysql:dbname=testdb;host=localhost", "root", "mysql1")

users = Table('users', db,
    Column('user_id', Integer(), primary_key=True),
    Column('name', String(40)),
    Column('age', Integer()),
    Column('password', String(200)),
)

users.create()
#users.insert(name='Mary', age='30', password='secret')
users.insert({'name': 'John', 'age':42, 'password':'secret'})

"""i = users.insert()
i.execute(name='Mary', age=30, password='secret')
i.execute({'name': 'John', 'age': 42},
          {'name': 'Susan', 'age': 57},
          {'name': 'Carl', 'age': 33})"""

"""#!/usr/bin/python <----QUESTION: what's this line for again?

import MySQLdb

# Open database connection
db = MySQLdb.connect("localhost","testuser","test123","TESTDB" )

# prepare a cursor object using cursor() method
cursor = db.cursor()

# Drop table if it already exist using execute() method.
cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")

# Create table as per requirement
sql =""" """CREATE TABLE EMPLOYEE (
         FIRST_NAME  CHAR(20) NOT NULL,
         LAST_NAME  CHAR(20),
         AGE INT,  
         SEX CHAR(1),
         INCOME FLOAT )"""

"""cursor.execute(sql)

# disconnect from server
db.close()"""


"""users = Table('users', metadata,
    Column('user_id', Integer, primary_key=True),
    Column('name', String(40)),
    Column('age', Integer),
    Column('password', String),
)
users.create()"""









