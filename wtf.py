from uhashring import HashRing
from collections import namedtuple
import psycopg2, base64, hashlib

clients = {
    "shard1": psycopg2.connect(
        host = "localhost",
        database = "postgres",
        port= "5433",
        user = "postgres",
        password= "postgres"
        ),
    "shard2": psycopg2.connect(
        host = "localhost",
        database = "postgres",
        port= "5434",
        user = "postgres",
        password= "postgres"
        ),
    "shard3": psycopg2.connect(
        host = "localhost",
        database = "postgres",
        port= "5435",
        user = "postgres",
        password= "postgres"
        ),
}


hr = HashRing(["shard1", "shard2", "shard3"])

url = "https://canaan.com"
u_hash = base64.b64encode(hashlib.sha256(url.encode('utf-8')).digest()).decode()
url_id = u_hash[:5]
server = hr.get(url_id)
s = clients.get(server.get('hostname'))
# s = "asd"
from pprint import pprint
d = None
with s as con:
    cur = con.cursor()
    cur.execute("SELECT now()")
    d = cur.fetchone()
pprint(d)