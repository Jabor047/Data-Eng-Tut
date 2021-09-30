from sqlalchemy import *
from config import host, database_name, user, password
conn_str = f"postgresql://{user}:{password}@{host}/{database_name}"
engine = create_engine(conn_str)
connection = engine.connect()
metadata = MetaData()
first_tb = Table('10AcademyB4Tutors', metadata,
   Column('id', Integer, primary_key=True),
   Column('name', String(255), nullable=False),
   Column('isHappy', Boolean, nullable=False)
)
metadata.create_all(engine)
query1 = insert(first_tb).values(id=1, name="Kevin", isHappy=True)
query2 = insert(first_tb).values(id=2, name="Mahlet", isHappy=False)
query3 = insert(first_tb).values(id=3, name="Yati", isHappy=True)
query4 = insert(first_tb).values(id=4, name="Tinsea", isHappy=True)
query5 = insert(first_tb).values(id=5, name="Cindy", isHappy=False)
# ResultProxy1 = connection.execute(query1)
# ResultProxy2 = connection.execute(query2)
ResultProxy5 = connection.execute(query5)

print(query5)
print(ResultProxy5)
