use("ca2");

// ############################################################################

// Part 1: Read (Find)

// Create six useful find queries on the movies collection. You should make use of:

// Comparison operators e.g. $in, $nin, $gt, $gte, $lte, $lt
// Array operators: e.g. $elemMatch, $all
// Projection
// Cursor methods e.g. limit, skip, sort and count

// Part 1, Query 1
// Find all French non-short comedic dramas that receive a viewer rating of 4 or higher on Rotten Tomatoes
// Display only title, directors, year, plot and runtime
// Sort results alphabetically by title and then by year released descending (if multiple films with the same title exist) and only display the first 10 entries
db.movies
  .find(
    {
      countries: "France",
      genres: { $ne: "Short", $all: ["Drama", "Comedy"] },
      "tomatoes.viewer.rating": { $gte: 4 },
    },
    { _id: 0, title: 1, genres: 1, directors: 1, year: 1, plot: 1, runtime: 1 }
  )
  .sort({ title: 1, year: -1 })
  .limit(10)
  .pretty();

// Part 1, Query 2
// Get count of films made in the past 25 years written or directed by both Ethan and Joel Coen and starring either Frances McDormand or George Clooney
db.movies
  .find({
    $or: [
      {
        writers: { $all: ["Ethan Coen", "Joel Coen"] },
      },
      {
        directors: { $all: ["Ethan Coen", "Joel Coen"] },
      },
    ],
    year: { $gt: new Date().getFullYear() - 25 },
    cast: { $in: ["Frances McDormand", "George Clooney"] },
  })
  .count();

// Part 1, Query 3
// Get films written and directed by Stanley Kubrick released between 1960 and 1980 where awards mentions "Oscar"
db.movies
  .find(
    {
      directors: "Stanley Kubrick",
      writers: /^Stanley Kubrick/,
      released: {
        $gte: new ISODate("1960-01-01T00:00:00Z"),
        $lt: new ISODate("1990-01-01T00:00:00Z"),
      },
      awards: { $all: [/^Won/, /Oscar/] },
    },
    { _id: 0, title: 1, year: 1, awards: 1, directors: 1, writers: 1 }
  )
  .sort({ year: 1 })
  .pretty();

// Part 1, Query 4
// Find films where a comment has been left by email address "warren_wilson@fakegmail.com" containing the word "voluptatibus" that was left between 1975 and 1985
// Display each film's title, directors, year and all comments, and skip the first two results
db.movies
  .find(
    {
      comments: {
        $elemMatch: {
          email: "warren_wilson@fakegmail.com",
          text: /voluptatibus/,
          date: {
            $gt: new ISODate("1975-01-01T00:00:00Z"),
            $lte: new ISODate("1985-01-01T00:00:00Z"),
          },
        },
      },
    },
    { _id: 0, title: 1, directors: 1, year: 1, comments: 1 }
  )
  .sort({ title: 1 })
  .skip(2)
  .pretty();

// Part 1, Query 5
// Find sub-ninety-minute films that are not rated "R" or "PG-13", were made in 1980 or later, are tagged with both "Animation" and "Adventure" genres, and have over 400 mflix comments
// Display film ID, title, num_mflix_comments, rating, year made and genres
// Sort the results by number of mflix comments descending, and limit results to first three films.
db.movies
  .find(
    {
      num_mflix_comments: { $gt: 400 },
      rated: { $nin: ["R", "PG-13"] },
      runtime: { $lt: 90 },
      year: { $gte: 1980 },
      genres: { $all: ["Animation", "Adventure"] },
    },
    {
      title: 1,
      num_mflix_comments: 1,
      rated: 1,
      runtime: 1,
      year: 1,
      genres: 1,
    }
  )
  .sort({ num_mflix_comments: -1 })
  .limit(3)
  .pretty();

// Part 1, Query 6
// Find all Harry Potter, Narnia, and Lord of the Rings films whose genres include "Adventure" and "Family" but not "Animation", and have a metacritic rating greater than or equal to sixty
// Display title, metacritic rating and genres, and sort by metacritic rating descending, limiting results to the first 5 films.
db.movies
  .find(
    {
      title: {
        $in: [
          /^Harry Potter/,
          /^The Chronicles of Narnia/,
          /^The Lord of the Rings/,
        ],
      },
      genres: { $ne: "Animation", $all: ["Adventure", "Family"] },
      metacritic: { $gte: 60 },
    },
    { _id: 0, title: 1, metacritic: 1, genres: 1 }
  )
  .sort({ metacritic: -1 })
  .limit(5)
  .pretty();

// ############################################################################
