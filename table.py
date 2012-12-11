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
			print sql

			cursor = self.dbh.cursor
			try:
				cursor.execute(sql)
				self.dbh.connection.commit()
			except:
				self.dbh.connection.rollback()

			#self.dbh.connection.close()

	def single_row(self):
		results =""

		sql = "select * from "+self.name+";"

		cursor = self.dbh.cursor

		try:
			cursor.execute(sql)
			results = Results(cursor.fetchone(), self.columns)
		except:
			self.dbh.connection.rollback()

		self.dbh.connection.close()

		return results

	def all_rows(self):
		results =""

		sql = "select * from "+self.name+";"

		cursor = self.dbh.cursor

		try:
			cursor.execute(sql)
			results = Results(cursor.fetchall(), self.columns)
		except:
			self.dbh.connection.rollback()

		self.dbh.connection.close()

		return results

class Results(object):
	def __init__(self, result, columns):
		print result
		self.result = result
		mapper={}

		#QUESTION: is it better to have a counter property on the "Column" object than to loop through the columns each time a "Results" object is created in order to make establish a index connection?
		
		#map the name of each column with its corresponding index i.e. {'user_id':0, 'name':1, 'age':2, 'password':3}
		#for idx, column in enumerate(columns):
		#	mapper[column.name] = idx

		self.mapper = mapper

	
	def __getitem__(self, x):
		if isinstance(x, str): #TODO: apparently it's more pythonic to do a try catch than to use isinstance
			return self.result[self.mapper[x]]
		else:
			return self.result[x]

class Column(object):
	def __init__(self, name, data_type, primary_key=False):
		self.type = data_type.val
		self.name = name
		self.primary_key = primary_key
		#i think that i want(should) to add some sort of counter here in order to have the ability to associated each column by index later

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

users.create()
#users.insert(name='Mary', age='30', password='secret')
#users.insert({'name': 'Mary', 'age':22, 'password':'guessit'})
users.insert(	{'name': 'Mary', 'age':22, 'password':'guessit'},
				{'name': 'Susan', 'age': 57},
          		{'name': 'Carl', 'age': 33})

#row = users.single_row() 
#rows = users.all_rows()

#for r in rows:
#	print r[1]

"""for r in row:
	print r["name"]
#s = users.select()
#rs = s.execute()

print 'Id:', row['user_id']
print 'Name:', row['name']
print 'Age:', row['age']
print 'Password:', row['password']"""
