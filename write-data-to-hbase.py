from starbase import Connection

c = Connection("127.0.0.1", "8000")

ratings = c.table('ratings')

if ( ratings.exists()):
    print("Drop existing table ratings \n")
    ratings.drop()

ratings.create('ratings')

print("Parsing the ml-100k ratings data \n")
ratingFile = open("c:/Users/Pramod/ml-100k/ml-100k/u.data", "r")

batch = ratings.batch()

for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}})

ratingFile.close()

print ("Commiting ratings data to HBase via REST service \n")
batch.commit(finalize=True)

print(ratings.fetch("1"))

ratings.drop()