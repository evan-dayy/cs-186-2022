// Task 3i

db.credits.aggregate([
    {$match: {"crew.id" : {$eq: 7624}}},
    {$project: {_id: 0, movieId: 1, cast: 1}},
    {$unwind: "$cast"},
    {$match: {"cast.id" : {$eq: 7624}}},
    {$project: {movieId: 1, character: "$cast.character"}},
    {$lookup: {
        from: "movies_metadata",
        localField: "movieId",
        foreignField: "movieId",
        as: "movie"
    }},
    {$sort: {"movie.release_date": -1}},
    {$project: {title: {$first : "$movie.title"}, 
                release_date: {$first : "$movie.release_date"}, 
                character: 1}}
    
]);