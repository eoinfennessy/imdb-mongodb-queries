use("ca2");


// ============================================================================
// Part 1: Read (Find)
// ============================================================================


// 1.1: Find all French non-short comedic dramas that receive a viewer rating of 4 or higher on Rotten Tomatoes
// Display only title, directors, year, plot and runtime
// Sort results alphabetically by title and then by year released descending (if multiple movies with the same title exist) and only display the first 10 entries
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

// 1.2: Get count of movies made in the past 25 years written or directed by both Ethan and Joel Coen and starring either Frances McDormand or George Clooney
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

// 1.3: Get movies written and directed by Stanley Kubrick released between 1960 and 1980 where awards mentions "Oscar"
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

// 1.4: Find movies where a comment has been left by email address "warren_wilson@fakegmail.com" containing the word "voluptatibus" that was left between 1975 and 1985
// Display each movie's title, directors, year and all comments, and skip the first two results
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

// 1.5: Find sub-ninety-minute movies that are not rated "R" or "PG-13", were made in 1980 or later, are tagged with both "Animation" and "Adventure" genres, and have over 400 mflix comments
// Display movie ID, title, num_mflix_comments, rating, year made and genres
// Sort the results by number of mflix comments descending, and limit results to first three movies.
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

// 1.6: Find all Harry Potter, Narnia, and Lord of the Rings movies whose genres include "Adventure" and "Family" but not "Animation", and have a metacritic rating greater than or equal to sixty
// Display title, metacritic rating and genres, and sort by metacritic rating descending, limiting results to the first 5 movies.
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


// ============================================================================
// Part 2: Create
// ============================================================================


// 2.1: Insert Movie 1 of 2
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
    // genres: ["Biography", "Comedy", "Drama"]
  },
  genres: ["Biography", "Comedy", "Drama"],
});

// 2.2: Insert Movie 2 of 2
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
    // genres: ["Comedy", "Crime", "Drama"]
  },
  genres: ["Comedy", "Crime", "Drama"],
});

// 2.3: Create "users" collection and add three entries
db.users.drop();
db.createCollection("users");

db.users.insertMany([
  {
    _id: 0,
    name: "Eoin Fennessy",
    email: "eoin@fennessy.com",
    password: "secret",
    joinDate: new Date("2012-03-01T12:15:31Z"),
    birthDate: new Date("1989-03-01"),
    favourites: [
      ObjectId("573a1397f29313caabce7b02"),
      ObjectId("573a139af29313caabcefe7e"),
    ],
  },
  {
    _id: 1,
    name: "Homer Simpson",
    email: "homer@simpson.com",
    password: "secret",
    joinDate: new Date("2015-07-03T15:12:01Z"),
    birthDate: new Date("12-05-1956"),
    favourites: [
      ObjectId("573a1398f29313caabceae08"),
      ObjectId("573a13b4f29313caabd410a2"),
      ObjectId("573a1398f29313caabce9ac0"),
      ObjectId("573a1397f29313caabce7b02"),
    ],
  },
  {
    _id: 2,
    name: "Marge Simpson",
    email: "marge@simpson.com",
    password: "secret",
    joinDate: new Date("2010-02-09T17:32:50Z"),
    birthDate: new Date("01-10-1956"),
    favourites: [
      ObjectId("573a1399f29313caabcecc6f"),
      ObjectId("573a1395f29313caabce1f6a"),
      ObjectId("573a13b4f29313caabd410a2"),
    ],
  },
]);


// ============================================================================
// Part 3: Update, Delete
// ============================================================================


// 3.1: Update the IMDB rating to a new value and increase the number of votes by 1.
db.movies.updateOne(
  { _id: 0 },
  {
    $set: { "imdb.rating": 8.3 },
    $inc: { "imdb.votes": 1 },
  }
);

// 3.2: Add a new favourite to the array in one of your user documents.
db.users.updateOne(
  { _id: 0 },
  { $addToSet: { favourites: ObjectId("573a1399f29313caabceeb20") } }
);

// 3.3: Update Tywin Lannister's email from "charles_dance@gameofthron.es" to "tywin_lannister@fakegmail.com" in all of his comments
db.movies.updateMany(
  { "comments.name": "Tywin Lannister" },
  { $set: { "comments.$[element].email": "tywin_lannister@fakegmail.com" } },
  { arrayFilters: [{ "element.email": "charles_dance@gameofthron.es" }] }
);

// 3.4: Add actor to the cast array for "Green Book"
db.movies.updateOne(
  { _id: 0 },
  {
    $push: { cast: "Sebastian Maniscalco" },
  }
);

// 3.5: Delete one of the movies you added
db.movies.deleteOne({ _id: 1 });


// ============================================================================
// Part 4: Aggregation
// ============================================================================


// 4.1: Group by countries each movie made after 1980 was made in and display various stats such as count of movies made and average IMDB rating for each
db.movies
  .aggregate([
    {
      $match: {
        year: { $gte: 1980 },
        "imdb.rating": { $ne: null },
      },
    },
    { $unwind: "$countries" },
    {
      $project: {
        countries: 1,
        "imdb.rating": 1,
        "imdb.votes": 1,
        runtime: 1,
        ageInYears: { $subtract: [new Date().getFullYear(), "$year"] },
      },
    },
    {
      $group: {
        _id: "$countries",
        avgRating: { $avg: "$imdb.rating" },
        sumOfVotes: { $sum: "$imdb.votes" },
        countOfMovies: { $count: {} },
        avgMovieAge: { $avg: "$ageInYears" },
        avgRuntime: { $avg: "$runtime" },
      },
    },
    { $match: { countOfMovies: { $gte: 50 } } },
    { $sort: { avgRating: -1 } },
    { $limit: 30 },
    {
      $addFields: {
        avgRating: { $round: ["$avgRating", 2] },
        avgMovieAge: { $round: ["$avgMovieAge", 1] },
        avgRuntime: { $round: "$avgRuntime" },
      },
    },
  ])
  .pretty();

// 4.2: Group by favourited movies and display count of times each was favourited and average age of users who favourited each and joined in or after 2010
db.users
  .aggregate([
    {
      $match: {
        joinDate: { $gte: new Date("2010-01-01") },
        favourites: { $ne: null },
      },
    },
    { $unwind: "$favourites" },
    {
      $project: {
        favourites: 1,
        userAge: {
          $divide: [{ $subtract: [new Date(), "$birthDate"] }, 31536000000],
        },
      },
    },
    {
      $lookup: {
        from: "movies",
        localField: "favourites",
        foreignField: "_id",
        as: "movie",
      },
    },
    {
      $group: {
        _id: "$movie.title",
        countOfFavourites: { $count: {} },
        avgUserAge: { $avg: "$userAge" },
      },
    },
    {
      $project: {
        _id: 0,
        movie: { $first: "$_id" },
        countOfFavourites: "$countOfFavourites",
        avgUserAge: { $round: ["$avgUserAge", 1] },
      },
    },
    {
      $sort: {
        countOfFavourites: -1,
        avgUserAge: 1,
        movie: 1,
      },
    },
    { $limit: 10 },
  ])
  .pretty();

// Undo all changes made to ca2 database
db.users.drop();
db.movies.deleteMany({ id: { $in: [0, 1] } });
db.movies.updateMany(
  { "comments.name": "Tywin Lannister" },
  { $set: { "comments.$[element].email": "charles_dance@gameofthron.es" } },
  { arrayFilters: [{ "element.email": "tywin_lannister@fakegmail.com" }] }
);
