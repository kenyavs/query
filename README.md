<h2>Query</h2>
An SQL rendering engine...moving towards automated persistance of Python objects, object realtional mapping.

<h2>Usage</h2>

SQL rendering engine:

```python
dbh = Connection("mysql:dbname=yourdatabasename;host=localhost", "yourusername", "yourpassword")
users = Table('users', dbh,
    Column('user_id', Integer(), primary_key=True),
    Column('name', String(40)),
    Column('age', Integer()),
    Column('password', String(200))
)
users.create()
users.insert(   {'name': 'Mary', 'age':22, 'password':'guessit'},
                {'name': 'Susan', 'age': 57},
                {'name': 'Carl', 'age': 33})
                
users.select(users['name', 'age'], [users['name'],'Mary'])
row = users.fetchone()

print "My name is "+row[0]+"and I'm " row["age"]+" years old."
users.select(users['name', 'age', 'password'])
rows = users.fetch()

for r in rows:
  print "My name is "+r[0]+" and I'm "+r['age']+" years old."
```

SQL Object Relation:
```python
usermapper = Mapper(User, users)

query = Query(User)

mary = query.fetchone([users['name'], 'Mary'])
mary.age = mary.age+1
query.save(mary)
```
