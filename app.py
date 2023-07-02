from fastapi import FastAPI
from uhashring import HashRing
import psycopg2, base64, hashlib

# this can go into config.toml
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

# hr = HashRing(clients)
hr = HashRing()
hr.add_node("shard1")
hr.add_node("shard2")
hr.add_node("shard3")

async def connect(driver, query:str = "SELECT now()"):
    d = None
    with driver as con:
        cur = con.cursor()
        cur.execute(query)
        d = cur.fetchone()
    return d


app = FastAPI()

@app.get("/")
async def handel_root(url_id: str):
    server = hr.get(url_id).get('hostname', 'shard3')
    d = await connect(clients.get(server), f"select * from url_table where url_id='{url_id}'")
    return {"res":d}

@app.post("/")
def handel_root(url:str):
    u_hash = base64.b64encode(hashlib.sha256(url.encode('utf-8')).digest()).decode()
    url_id = u_hash[:5]
    server = hr.get(url_id).get('hostname', 'shard3')
    # return clients
    database = clients.get(server)

    try:
        with database as conn:
            cur = conn.cursor()
            cur.execute(f"INSERT INTO url_table(url, url_id) VALUES ('{url}', '{url_id}')")
        return {"inserted into: ": server, "url_id": url_id}
    except Exception as ex:
        return {
            "ex":ex
            }


