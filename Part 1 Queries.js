// --------------------------
// Question 1
// --------------------------
db = db.getSiblingDB("global_cities")

db.countries_states_cities.aggregate([
  {
    $group: {
      _id: { subregion: "$subregion", region: "$region" },
      "number of countries": { $sum: 1 }
    }
  },
  {
    $match: {
      "number of countries": { $gte: 10 }
    }
  },
  {
    $project: {
      _id: 0,
      region: "$_id.region",
      subregion: "$_id.subregion",
      "number of countries": 1
    }
  },
  {
    $sort: { "number of countries": -1 }
  }
])

// Q1 answer: South America has 15 countries, Northern Europe has 16.

// --------------------------
// Question 2
// --------------------------
db = db.getSiblingDB("global_cities")

db.countries_states_cities.aggregate([
  {
    $unwind: "$states"
  },
  {
    $unwind: "$states.cities"
  },
  {
    $match: { "states.cities.name": "Rochester" }
  },
  {
    $project: {
      _id: 0,
      country: "$name",
      state: "$states.name",
      city: "$states.cities.name",
      latitude: "$states.cities.latitude",
      longitude: "$states.cities.longitude"
    }
  }
])

// Q2 answer: Ten states in the United States contain a city named Rochester. Rochester, Washington is located furthest north with a latitude of 46.82177000.

// --------------------------
// Question 3
// --------------------------
db = db.getSiblingDB("global_cities")

db.countries_states_cities.aggregate([
  {
    $match: { currency: "USD" }
  },
  {
    $unwind: "$states"
  },
  {
    $unwind: "$states.cities"
  },
  {
    $group: {
      _id: { country: "$name", currency: "$currency" },
      cityCount: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      country: "$_id.country",
      currency: "$_id.currency",
      "total number of cities": "$cityCount"
    }
  },
  {
    $sort: { "total number of cities": -1 }
  }
])

// Q3 answer: Ecuador has the second most cities listed with 115 cities.

// --------------------------
// Question 4
// --------------------------
db = db.getSiblingDB("yelp")
db.businesses.aggregate([
    {
        $match: {
            state: "CA",
            categories: "Restaurants",
            "attributes.OutdoorSeating": "True"
        }
    },
    {
        $group: {
            _id: "$city",
            count: { $sum: 1 }
        }
    },
    {
        $match: {
            count: { $gte: 10 }
        }
    },
    {
        $sort: { count: -1 }
    },
    {
        $project: {
            _id: 0,
            "Number of restaurants with outdoor seating": "$count",
            "City": "$_id"
        }
    }
])

// Q4 answer: Cities in California with more than ten restaurants offering outdoor seating are all located in coastal areas in Southern California, specifically surrounding Santa Barbara.

// --------------------------
// Question 5
// --------------------------
db = db.getSiblingDB("yelp")
db.businesses.aggregate([
    {
        $match: {
            city: "Philadelphia",
            categories: "Dim Sum",
            "attributes.GoodForKids": "True",
            "attributes.WheelchairAccessible": "True",
            stars: { $gte: 4 },
            review_count: { $gte: 250 }
        }
    },
    {
        $project: {
            _id: 0,
            "Restaurant name": "$name",
            "Full address": {
                $concat: [
                    "$address", ", ",
                    "$city", ", ",
                    "$state", ", ",
                    "$postal_code"
                ]
            },
            "Star rating": "$stars"
        }
    }
])

// Q5 answer: Based on the solely results returned, the 19107 zip code in Philadelphia is most likely to contain Philadelphia's Chinatown neighborhood as it contains 3/7 of the returned results.

// --------------------------
// Question 6
// --------------------------
db = db.getSiblingDB("yelp")
db.businesses.aggregate([
    {
        $match: {
            categories: { $all: ["Yoga", "Meditation Centers"] }
        }
    },
    {
        $group: {
            _id: "$state",
            count: { $sum: 1 }
        }
    },
    {
        $sort: { count: -1 }
    },
    {
        $limit: 5
    },
    {
        $project: {
            _id: 0,
            "Number of Yoga and Meditation Centers": "$count",
            "State": "$_id"
        }
    }
])

// Q6 answer: Based on these results, there appear to be more Yoga and Meditation Centers in Florida than in California.

// --------------------------
// Question 7
// --------------------------
db = db.getSiblingDB("yelp")
db.users.aggregate([
    {
        $match: {
            name: { $in: ["Jennifer", "Jenny"] }
        }
    },
    {
        $group: {
            _id: "$name",
            total_users: { $sum: 1 },
            total_reviews: { $sum: "$review_count" },
            total_useful: { $sum: "$useful" },
            total_funny: { $sum: "$funny" },
            total_cool: { $sum: "$cool" }
        }
    },
    {
        $project: {
            _id: 0,
            "Name": "$_id",
            "Number of users": "$total_users",
            "Total reviews written": "$total_reviews",
            "Average reviews per user": {
                $round: [{ $divide: ["$total_reviews", "$total_users"] }, 2]
            },
            "Average useful votes per user": {
                $round: [{ $divide: ["$total_useful", "$total_users"] }, 2]
            },
            "Average funny votes per user": {
                $round: [{ $divide: ["$total_funny", "$total_users"] }, 2]
            },
            "Average cool votes per user": {
                $round: [{ $divide: ["$total_cool", "$total_users"] }, 2]
            }
        }
    },
    {
        $sort: { "Name": 1 }
    }
])

// Q7 answer: There are more users named Jennifer (13275) than Jenny (2920) in the dataset. Jennifers have written 354753 reiviews, while Jennys have written 99898. Jennys write more reviews on average (approximately 34 reviews per user) compared to Jennifers (approximately 27 reviews per user). Jennys are also more likely to vote other people's reviews as useful, cool, or funny, with higher total average votes across all three categories.

// --------------------------
// Question 8
// --------------------------
db = db.getSiblingDB("sample_airbnb")
db.listingsAndReviews.aggregate([
    {
        $match: {
            "address.market": { $in: ["Barcelona", "Porto", "Sydney", "New York", "Montreal", "Istanbul"] }
        }
    },
    {
        $group: {
            _id: "$address.market",
            max_price: { $max: "$price" },
            min_price: { $min: "$price" },
            avg_price: { $avg: "$price" },
            avg_cleaning: { $avg: "$cleaning_fee" }
        }
    },
    {
        $sort: { _id: 1 }
    },
    {
        $project: {
            _id: 0,
            "Highest nightly price": "$max_price",
            "Average nightly price": "$avg_price",
            "Lowest nightly price": "$min_price",
            "Average cleaning fee": "$avg_cleaning",
            "Market": "$_id"
        }
    }
])

// Q8 answer: Based on these returned results, Porto seems to be the most affordable market, with the lowest average nightly price and by far the lowest average cleaning fees of the six markets analyzed.

// --------------------------
// Question 9
// --------------------------
db = db.getSiblingDB("sample_airbnb")
db.listingsAndReviews.aggregate([
    {
        $match: {
            "address.market": "Porto"
        }
    },
    {
        $group: {
            _id: "$host.host_id",
            host_name: { $first: "$host.host_name" },
            host_location: { $first: "$host.host_location" },
            listing_count: { $sum: 1 },
            avg_price: { $avg: "$price" }
        }
    },
    {
        $sort: { listing_count: -1 }
    },
    {
        $limit: 3
    },
    {
        $project: {
            _id: 0,
            "Host name": "$host_name",
            "Host location": "$host_location",
            "Average nightly price": "$avg_price",
            "Number of listings in Porto": "$listing_count",
            "Host ID": "$_id"
        }
    }
])

// Q9 answer: Based on these results, the Airbnb rental market in Porto appears to be dominated by a small number of larger rental companies, as the top three hosts each have multiple listings with corporate sounding names like "Feels Like Home," suggesting property management companies rather than individual landlords (we'd expect to see host names with actual first and last names if it was dominated by individual landlords).

// --------------------------
// Question 10
// --------------------------
db = db.getSiblingDB("sample_airbnb")

db.listingsAndReviews.aggregate([
  {
    $match: { "address.market": "Montreal" }
  },
  {
    $unwind: "$reviews"
  },
  {
    $project: {
      reviewerId: "$reviews.reviewer_id",
      reviewerName: "$reviews.reviewer_name"
    }
  },
  {
    $group: {
      _id: "$reviewerId",
      reviewerName: { $first: "$reviewerName" },
      reviewCount: { $sum: 1 }
    }
  },
  {
    $match: { reviewCount: { $gte: 5 } }
  },
  {
    $project: {
      _id: 0,
      "Reviewer name": "$reviewerName",
      "Reviewer ID": "$_id",
      "Number of reviews": "$reviewCount"
    }
  },
  {
    $sort: { "Number of reviews": -1 }
  }
])

// Q10 answer: There are 6 reviewers who have written five or more reviews for listings in the Montreal market.

// --------------------------
// Question 11
// --------------------------
db = db.getSiblingDB("nba")
db.games.aggregate([
    {
        $match: {
            "box.players.player": { $in: ["Kobe Bryant", "Tim Duncan"] }
        }
    },
    {
        $unwind: "$box"
    },
    {
        $unwind: "$box.players"
    },
    {
        $match: {
            "box.players.player": { $in: ["Kobe Bryant", "Tim Duncan"] }
        }
    },
    {
        $group: {
            _id: {
                game_id: "$_id",
                player: "$box.players.player"
            }
        }
    },
    {
        $group: {
            _id: "$_id.player",
            games_played: { $sum: 1 }
        }
    },
    {
        $sort: { games_played: -1 }
    },
    {
        $project: {
            _id: 0,
            "Player": "$_id",
            "Games played": "$games_played"
        }
    }
])
// Q11 answer: Kobe Bryant played in more NBA games (1248) than Tim Duncan (1188) during the dates covered by this dataset.

// --------------------------
// Question 12
// --------------------------
db = db.getSiblingDB("nba")
db.games.aggregate([
    {
        $match: {
            date: {
                $gte: ISODate("2003-07-01T00:00:00.000Z"),
                $lte: ISODate("2004-06-30T23:59:59.999Z")
            }
        }
    },
    {
        $match: {
            $expr: {
                $or: [
                    {
                        $and: [
                            { $eq: [{ $arrayElemAt: ["$teams.won", 0] }, 0] },
                            { $lt: [{ $arrayElemAt: ["$teams.score", 0] }, 70] }
                        ]
                    },
                    {
                        $and: [
                            { $eq: [{ $arrayElemAt: ["$teams.won", 1] }, 0] },
                            { $lt: [{ $arrayElemAt: ["$teams.score", 1] }, 70] }
                        ]
                    }
                ]
            }
        }
    },
    {
        $project: {
            _id: 0,
            "Date": "$date",
            "Team 1": { $arrayElemAt: ["$teams.name", 0] },
            "Team 1 score": { $arrayElemAt: ["$teams.score", 0] },
            "Team 2": { $arrayElemAt: ["$teams.name", 1] },
            "Team 2 score": { $arrayElemAt: ["$teams.score", 1] }
        }
    },
    {
        $sort: { "Date": 1 }
    }
])

// Q12 answer: Based on these results, teams scoring less than 70 points in an NBA game is quite rare. With approximately 1,230 games in a typical NBA season, and only 47 games meeting this criteria in the 03-04 season, we can estimate the losing team scoring less than 70 points occurs in about 3% of all NBA games.

// --------------------------
// Question 13
// --------------------------

// Q13 answer: The results indicate a moderate home-court advantage exists in the NBA, with home teams winning approximately 60% of games between July 1995 and June 2005, which represents a statistically significant advantage compared to the 50% that would be expected if there were no home-court effect.

db = db.getSiblingDB("nba")
db.games.aggregate([
    {
        $match: {
            date: {
                $gte: ISODate("1995-07-01T00:00:00.000Z"),
                $lte: ISODate("2005-06-30T23:59:59.999Z")
            }
        }
    },
    {
        $project: {
            home_won: {
                $cond: [
                    {
                        $and: [
                            { $eq: [{ $arrayElemAt: ["$teams.home", 0] }, true] },
                            { $eq: [{ $arrayElemAt: ["$teams.won", 0] }, 1] }
                        ]
                    },
                    1,
                    {
                        $cond: [
                            {
                                $and: [
                                    { $eq: [{ $arrayElemAt: ["$teams.home", 1] }, true] },
                                    { $eq: [{ $arrayElemAt: ["$teams.won", 1] }, 1] }
                                ]
                            },
                            1,
                            0
                        ]
                    }
                ]
            }
        }
    },
    {
        $group: {
            _id: null,
            total_games: { $sum: 1 },
            home_wins: { $sum: "$home_won" }
        }
    },
    {
        $project: {
            _id: 0,
            total_games: 1,
            home_wins: 1,
            home_win_percentage: {
                $multiply: [
                    { $divide: ["$home_wins", "$total_games"] },
                    100
                ]
            }
        }
    }
])

// --------------------------
// Question 14
// --------------------------

// Q14 answer: The analysis shows that teams with more steals in a game won approximately 60% of those games during the 2000s, suggesting a moderately strong positive correlation between having more steals and winning the game, likely because steals create additional scoring opportunities and indicate stronger defensive performance.

db = db.getSiblingDB("nba")
db.games.aggregate([
    {
        $match: {
            date: {
                $gte: ISODate("2000-01-01T00:00:00.000Z"),
                $lte: ISODate("2010-12-31T23:59:59.999Z")
            }
        }
    },
    {
        $project: {
            team1_steals: { $arrayElemAt: ["$box.team.stl", 0] },
            team2_steals: { $arrayElemAt: ["$box.team.stl", 1] },
            team1_won: { $arrayElemAt: ["$teams.won", 0] },
            team2_won: { $arrayElemAt: ["$teams.won", 1] }
        }
    },
    {
        $project: {
            higher_steals_won: {
                $cond: [
                    { $gt: ["$team1_steals", "$team2_steals"] },
                    "$team1_won",
                    {
                        $cond: [
                            { $gt: ["$team2_steals", "$team1_steals"] },
                            "$team2_won",
                            -1
                        ]
                    }
                ]
            }
        }
    },
    {
        $match: {
            higher_steals_won: { $ne: -1 }
        }
    },
    {
        $group: {
            _id: null,
            total_games: { $sum: 1 },
            higher_steals_wins: { $sum: "$higher_steals_won" }
        }
    },
    {
        $project: {
            _id: 0,
            total_games: 1,
            higher_steals_wins: 1,
            win_percentage: {
                $multiply: [
                    { $divide: ["$higher_steals_wins", "$total_games"] },
                    100
                ]
            }
        }
    }
])
