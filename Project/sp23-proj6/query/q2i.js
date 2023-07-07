// Task 2i

db.movies_metadata.aggregate([
    {$project:
        {
            _id: 0,
            title: 1,
            vote_count: 1,
            score : {
                $add: [
                    {$multiply: ["$vote_average", {$round : {$divide: ["$vote_count", {$add: ["$vote_count", 1838]}]}}]},
                    {$multiply: [7, {$round : {$divide: [1838, {$add: ["$vote_count", 1838]}]}}]},
                ]
            }
        }
    },
    {$sort: {score: -1}},
    {$limit: 20}
]);