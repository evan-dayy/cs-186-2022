// Task 3ii

db.credits.aggregate([
    {$match: 
        {crew: 
            {$elemMatch: {
                id : {$eq: 5655},
                job : {$eq: "Director"}
            }
        }}
    },
    {$project: {_id: 0, cast: 1}},
    {$unwind: "$cast"},
    {$group: {_id: {val1: "$cast.id", val2: "$cast.name"}, count: {$sum: 1}}},
    {$project: {
        _id: 0,
        count: 1,
        id: "$_id.val1",
        name: "$_id.val2"
    }},
    {$sort: {count: -1, id: 1}},
    {$limit: 5}
]);