from fastapi import FastAPI, HTTPException, Header
import psycopg2
import pandas as pd

# create FastAPI object
app = FastAPI()

#api key
password = "kiamat2012"


def getConnection():
    # create connection
    conn = psycopg2.connect(
        dbname="neondb", user="neondb_owner", password="npg_sLfVg8iW4EwO",
        host="ep-steep-water-a102fmjl-pooler.ap-southeast-1.aws.neon.tech",
    )

    return conn

# endpoint (untuk mengambil data halaman utama)
@app.get('/')
def getWelcome(): #function handler
    return {
        "msg": "sample-fastapi-pg"
    }

# endpoint (mengambil data dari database) endpoint protected
@app.get('/profile')
def getProfiles():
    #definition connection
    connection = getConnection()
    #get data dari database
    df = pd.read_sql("select*from profiles", connection)
    
    return {
        "data": df.to_dict(orient="records"),
        "columns": list(df.columns)

    } 


# endpoint protected
@app.get('/profile/{id}/{name}')
def getProfileById(id: int, name: str, api_key: str = Header(None)):
    #cek crendential
    if api_key == None or api_key != password:
        #kasih error
        raise HTTPException(status_code=401, detail="password salah")

    #define connection
    connection = getConnection()
    #get data dari database
    df = pd.read_sql(f"select * from profiles where id = {id}", connection)
    #filter langsung dari csv
    df = df.query(f"id == {id} and name == '{name}'")

    if len(df) == 0:
        raise HTTPException(status_code=404, detail="data not found")

    return {
        "data": df.to_dict(orient="records"),
        "columns": list(df.columns)
     }





# @app.post(...)
# async def createProfile():
#     pass


# @app.patch(...)
# async def updateProfile():
#     pass


# @app.delete(...)
# async def deleteProfile():
#     pass
