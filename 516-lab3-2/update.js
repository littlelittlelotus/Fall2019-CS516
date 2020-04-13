db.movies.updateMany({rating: {$gte: 4}}, {$set: {"summary": "great"}})
db.movies.updateMany({rating: {$lt: 2}}, {$set: {"summary": "bad"}})
db.movies.updateMany({$and: [{rating: {$lt: 4}},{rating: {$gte: 2}}]}, {$set: {"summary": "ok"}})
