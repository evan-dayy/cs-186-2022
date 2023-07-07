// Task 1i

db.keywords.aggregate([
    {
        $match: {$or : [{keywords: {$elemMatch: {name : "mickey mouse"}}}, 
                     {keywords: {$elemMatch: {name : "marvel comic"}}}]}
    },
    {
        $lookup: {
            from: "movies_metadata",
            localField: "movieId",
            foreignField: "movieId",
            as : "movies"
        }
    },
    {$sort: {movieId: 1}},
    {
        $project: {
            _id: 0,
            movieId : 1
        }
    }
    
]);