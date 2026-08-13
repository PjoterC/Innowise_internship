// Queries for the `reviews_db.reviews` collection loaded by the
// `reviews_to_mongo` DAG from tiktok_google_play_reviews.csv.
// Run it with the wrapper in this directory (uses the mongosh inside the
// mongo:7 container, so nothing needs installing on the host):
//   ./run_mongo_queries.sh
// With mongosh installed on the host, the port is published, so this works too:
//    mongosh "mongodb://mongo:mongo@localhost:27017/?authSource=admin" mongo_queries.js 


const reviewsDb = db.getSiblingDB("reviews_db");

// Top 5 most frequently occurring comments.
//
// Grouping is done on a normalised key (trimmed + lower-cased) so that
// "Nice app", "nice app" and " Nice app " count as the same comment; the
// original spelling of the first occurrence is kept for display.

print("\nMost frequent comments:\n")
const topComments = reviewsDb.reviews
  .aggregate([
    {
      $match: {
        content: { $type: "string", $nin: ["", "-"] },
      },
    },
    {
      $group: {
        _id: { $toLower: { $trim: { input: "$content" } } },
        count: { $sum: 1 },
        example: { $first: "$content" },
        totalThumbsUp: { $sum: "$thumbsUpCount" },
        avgScore: { $avg: "$score" },
      },
    },
    { $sort: { count: -1, _id: 1 } },
    { $limit: 5 },
    {
      $project: {
        _id: 0,
        comment: "$example",
        count: 1,
        totalThumbsUp: 1,
        avgScore: { $round: ["$avgScore", 2] },
      },
    },
  ])
  .toArray();

printjson(topComments);

// All entries whose `content` is shorter than 5 characters.

print("\nShort content:\n")
const shortContent = reviewsDb.reviews
  .find(
    {
      $expr: {
        $and: [
          { $eq: [{ $type: "$content" }, "string"] },
          { $lt: [{ $strLenCP: "$content" }, 5] },
        ],
      },
    },
    { _id: 0, reviewId: 1, userName: 1, content: 1, score: 1, at: 1 },
  )
  .toArray();

print(`entries with content shorter than 5 chars: ${shortContent.length}`);
printjson(shortContent.slice(0, 10)); // preview; drop the slice for the full set

// Average rating per day, keyed by a real BSON date (not a formatted string).
//

print("\nAverage rating per day:\n")
const avgScorePerDay = reviewsDb.reviews
  .aggregate([
    {
      $addFields: {
        day: {
          $dateTrunc: {
            date: {
              $dateFromString: {
                dateString: "$at",
                format: "%Y-%m-%d %H:%M:%S",
                onError: null,
                onNull: null,
              },
            },
            unit: "day",
          },
        },
      },
    },
    { $match: { day: { $ne: null } } },
    {
      $group: {
        _id: "$day",
        avgScore: { $avg: "$score" },
        reviews: { $sum: 1 },
      },
    },
    { $sort: { _id: 1 } },
    {
      $project: {
        _id: 0,
        day: "$_id", // BSON date — midnight UTC of that day
        avgScore: { $round: ["$avgScore", 2] },
        reviews: 1,
      },
    },
  ])
  .toArray();

printjson(avgScorePerDay);
