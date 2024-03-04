import pymongo
from pymongo import MongoClient
client = MongoClient("localhost", 27017)

db = client["laba5"]
movies = db["movies"]

new_movies = [
    {
        "title": "Terminator 2: Judgement Day",
        "year": "1991",
        "country": "USA",
    },
    {
        "title": "Sherlock Holmes: A Shadows Games",
        "year": "2011",
        "country": "USA",
    },
    {
        "title": "Demon Slayer - The Movie: Mugen Train",
        "year": "2020",
        "country": "Japan",
    },
]

result = movies.insert_many(new_movies)

print(movies.find_one({"title": "Terminator 2: Judgement Day"}, {"_id": 0, "year": 1}))

countries = movies.distinct("country")

for c in countries:
    print(c, movies.count_documents({"country": c}))

last_movie = movies.find({}, {"_id": 0, "year": 1}).limit(1).sort({"$natural":-1})[0]

for m in movies.find({"year": {"$lt": last_movie["year"]}}, {"_id": 0}):
    print(m)

movies.drop()
