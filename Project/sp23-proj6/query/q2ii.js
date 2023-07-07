// Task 2ii

db.movies_metadata.aggregate([
    {$project: {_id: 0, tagline: {$split: ["$tagline", " "]}}},
    {$unwind: "$tagline"},
    {$project: {
        tagline: {$trim: {input: "$tagline", chars: " .,!?'`"}}
    }},
    {$project: {
        tagline: {$toLower: "$tagline"},
        length: {$strLenCP: "$tagline"}
    }},
    {$match: {
        length : {$gt: 3}
    }},
    {$group: {
        _id: "$tagline",
        count: {$sum: 1}
    }},
    {$sort: {count: -1, _id: 1}},
    {$limit: 20}
]);