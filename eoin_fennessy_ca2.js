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

// Part 2: Create

// The movies collection only contains movies from 2016 and earlier. Add two movies you like that were released between 2017 and 2021. The movie documents you create should contain:

// _id (int)
// title (string)
// year (int)
// runtime (int)
// cast (array)
// plot (string)
// directors (array)
// imdb (subdocument, containing…)
//   rating (double)
//   votes (int)
//   genres (array)

//Part 2, Insert Movie 1 of 2
db.movies.deleteOne({ _id: 0 });
db.movies.insertOne({
  _id: 0,
  title: "Green Book",
  year: 2018,
  runtime: 130,
  cast: ["Viggo Mortensen", "Mahershala Ali", "Linda Cardellini"],
  plot: "A working-class Italian-American bouncer becomes the driver for an African-American classical pianist on a tour of venues through the 1960s American South.",
  directors: ["Peter Farrelly"],
  imdb: {
    rating: 8.2,
    votes: 493411,
    //   genres: [
    //   "Biography",
    //   "Comedy",
    //   "Drama"
    // ]
  },
  genres: ["Biography", "Comedy", "Drama"],
});

//Part 2, Insert Movie 2 of 2
db.movies.deleteOne({ _id: 1 });
db.movies.insertOne({
  _id: 1,
  title: "Three Billboards Outside Ebbing, Missouri",
  year: 2017,
  runtime: 115,
  cast: ["Frances McDormand", "Woody Harrelson", "Sam Rockwell"],
  plot: "A mother personally challenges the local authorities to solve her daughter's murder when they fail to catch the culprit.",
  directors: ["Martin McDonagh"],
  imdb: {
    rating: 8.1,
    votes: 509092,
    //   genres: [
    //   "Comedy",
    //   "Crime",
    //   "Drama"
    // ]
  },
  genres: ["Comedy", "Crime", "Drama"],
});

//Part 2, Create "users" collection and add three entries
db.users.drop();
db.createCollection("users");

db.users.deleteMany({ id: { $in: [0, 1, 2] } });
db.users.insertMany([
  {
    _id: 0,
    favourites: [
      ObjectId("573a1397f29313caabce7b02"), 
      ObjectId("573a139af29313caabcefe7e")
    ],
    name: "Eoin Fennessy",
    email: "eoin@fennessy.com",
    password: "secret",
    joinDate: { $date: "2012-03-01T12:15:31Z" },
  },
  {
    _id: 1,
    favourites: [
      ObjectId("573a1398f29313caabceae08"),
      ObjectId("573a13b4f29313caabd410a2"),
      ObjectId("573a1398f29313caabce9ac0"),
    ],
    name: "Homer Simpson",
    email: "homer@simpson.com",
    password: "secret",
    joinDate: { $date: "2015-07-03T15:12:01Z" },
  },
  {
    _id: 2,
    favourites: [
      ObjectId("573a1399f29313caabcecc6f"),
      ObjectId("573a1395f29313caabce1f6a")
    ],
    name: "Marge Simpson",
    email: "marge@simpson.com",
    password: "secret",
    joinDate: { $date: "2010-02-09T17:32:50Z" },
  },
]);

// ############################################################################

// Part 3: Update, Delete

// Update
// Perform the following update operations on the movies and users you added:

// On one of your movie documents (whichever you choose), update the IMDB rating to a new value and increase the number of votes by 1.
// Add a new favourite to the array in one of your user documents.
// Perform two additional updates of your choice.

// Delete
// Write the code to delete one of the movies you added.

// Part 3: Update the IMDB rating to a new value and increase the number of votes by 1.
db.movies.updateOne(
  { _id: 0 },
  {
    $set: { "imdb.rating": 8.3 },
    $inc: { "imdb.votes": 1 },
  }
);

// Part 3: Add a new favourite to the array in one of your user documents.
db.users.updateOne(
  { _id: 0 },
  { $addToSet: { favourites: "573a1399f29313caabceeb20" } }
);

// Part 3: Update Tywin Lannister's email from "charles_dance@gameofthron.es" to "tywin_lannister@fakegmail.com" in all of his comments
db.movies.updateMany(
  { "comments.name": "Tywin Lannister" },
  { $set: { "comments.$[element].email": "tywin_lannister@fakegmail.com" } },
  { arrayFilters: [{ "element.email": "charles_dance@gameofthron.es" }] }
);

// Part 3: Add actor to the cast array for "Green Book"
db.movies.updateOne(
  { _id: 0 },
  {
    $push: { cast: "Sebastian Maniscalco" },
  }
);

// Part 3: Delete one of the movies you added
db.movies.deleteOne({ _id: 1 });
