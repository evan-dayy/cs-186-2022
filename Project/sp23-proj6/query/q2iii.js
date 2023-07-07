// Task 2iii
db.movies_metadata.aggregate([
    {$project : {_id: 0, budget: 1}},
    {$match: {$and: [{budget : {$ne: false}}, 
                    {budget :{$ne: 0}}, 
                    {budget :{$ne: null}},
                    {budget :{$ne: ''}}]}},
    {$project: {budget: 
        {$cond: {if: {$isNumber: '$budget'}, 
                then: '$budget', 
                else: {$toInt: {$trim: {input: '$budget', chars: 'USD\$ '}}}}}}
            },
    {$project: {budget: {$round: ['$budget', -7]}}},
    {$group: {_id: "$budget", count: {$sum: 1}}},
    {$project: {_id: 0, budget: "$_id", count: 1}},
    {$sort: {budget: 1}}
]);