/** @format */

// region: CONSTANTS
const DB_USER = "root";
const DB_PASSWORD = "super_duper_password";
const DB_NAME = "big_data_hw4";
const RESTAURANT_TABLE = "restaurants";
const DURHAM_RESTAURANTS_TABLE = "durham_restaurants";
const DURHAM_FORECLOSURE_TABLE = "durham_nc_foreclosure";
const METEORITES_TABLE = "meteorites";
const WORLD_CITIES_TABLE = "worldcities";
// endregion: CONSTANTS

// region: Preliminary
// * get the database intance
const db = new Mongo(`mongodb://${DB_USER}:${DB_PASSWORD}@mongo:27017/`).getDB(DB_NAME);
// endregion: Preliminary

// region: Q1
const restaurantCollection = db[RESTAURANT_TABLE];

// * 1. Count the number of documents in the restaurants collection
const restaurantCount = restaurantCollection.countDocuments({});
print(`There are ${restaurantCount} documents in the ${RESTAURANT_TABLE} collection`);

// * 2. Display all the documents in the collection
restaurantCollection.find({}).forEach(printjson);

// * 3. Display: restaurant_id, name, borough and cuisine for all the documents
restaurantCollection.find({}, { restaurant_id: 1, name: 1, borough: 1, cuisine: 1 }).forEach(printjson);

// * 4. Display: restaurant_id, name, borough and cuisine, but exclude field _id
restaurantCollection.find({}, { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 }).forEach(printjson);

// * 5. Display: restaurant_id, name, borough and zip code, exclude the field _id
restaurantCollection
  .find({}, { restaurant_id: 1, name: 1, borough: 1, "address.zipcode": 1, _id: 0 })
  .forEach(printjson);

// * 6. Display all the restaurants in the Bronx
restaurantCollection.find({ borough: "Bronx" }).forEach(printjson);

// * 7. Display the first 5 restaurants in the Bronx
restaurantCollection.find({ borough: "Bronx" }).limit(5).forEach(printjson);

// * 8. Display the second 5 restaurants in the Bronx (skip the first 5)
restaurantCollection.find({ borough: "Bronx" }).skip(5).limit(5).forEach(printjson);

// * 9. Find the restaurants with any score more than 85
restaurantCollection.find({ "grades.score": { $gt: 85 } });

// * 10. Find the restaurants that achieved score, more than 80 but less than 100
restaurantCollection.find({ "grades.score": { $gt: 80, $lt: 100 } });

// * 11. Find the restaurants which locate in longitude value less than -95.754168
restaurantCollection.find({ "address.coord.0": { $lt: -95.754168 } });

// * 12. Find the restaurants that do not prepare any cuisine of 'American'
// * and their grade score more than 70 and longitude less than -65.754168
restaurantCollection.find({
  $and: [
    { cuisine: { $ne: "American " } },
    { "grades.score": { $gt: 70 } },
    { "address.coord.0": { $lt: -65.754168 } },
  ],
});

// * 13. Find the restaurants which do not prepare any cuisine of 'American'
// * and achieved a score more than 70
// * and located in the longitude less than -65.754168
// * (without using $and operator).
restaurantCollection.find({
  cuisine: { $ne: "American " },
  "grades.score": { $gt: 70 },
  "address.coord.0": { $lt: -65.754168 },
}); // * here since we are operating on different fields, Mongo implicitly uses $and to join the conditions

// * 14. Find the restaurants which do not prepare any cuisine of 'American'
// * and achieved a grade point 'A'
// * and not in the borough of Brooklyn
// * sorted by cuisine in descending order.
db.restaurants
  .find({
    cuisine: { $ne: "American " },
    "grades.grade": "A",
    borough: { $ne: "Brooklyn" },
  })
  .sort({ cuisine: -1 });

// * 15. Find the restaurant Id, name, borough, and cuisine for those restaurants
// * which contain 'Wil' as the first three letters of its name.
restaurantCollection.find(
  {
    name: /^Wil/,
  },
  {
    restaurant_id: 1,
    name: 1,
    borough: 1,
    cuisine: 1,
    _id: 0,
  },
);

// * 16. Find the restaurant Id, name, borough, and cuisine for those restaurants
// * which contain 'ces' as the last three letters of its name.
restaurantCollection.find({ name: /ces$/ }, { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 });

// * 17. Find the restaurant Id, name, borough, and cuisine for those restaurants
// * which contain 'Reg' as three letters somewhere in its name.
restaurantCollection.find({ name: /Reg/ }, { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 });

// * 18. Find the restaurants which belong to the borough Bronx
// * and prepare either American or Chinese dishes.
restaurantCollection.find({
  borough: "Bronx",
  $or: [{ cuisine: "American " }, { cuisine: "Chinese" }],
});

// * 19. Find the restaurant Id, name, borough, and cuisine for those restaurants
// * which belong to the boroughs of Staten Island or Queens or Bronx or Brooklyn.
restaurantCollection.find(
  { borough: { $in: ["Staten Island", "Queens", "Bronx", "Brooklyn"] } },
  { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 },
);

// * 20. Find the restaurant Id, name, borough, and cuisine for those restaurants
// * which are not belonging to the borough Staten Island or Queens or Bronx or Brooklyn
restaurantCollection.find(
  { borough: { $nin: ["Staten Island", "Queens", "Bronx", "Brooklyn"] } },
  { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 },
);

// * 21. Find the restaurant Id, name, borough, and cuisine for those restaurants
// * which achieved a score below 10.
restaurantCollection.find(
  { "grades.score": { $lt: 10 } },
  { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 },
);

// * 22. Find the restaurant Id, name, borough, and cuisine for those restaurants
// * which prepared dishes except 'American' and 'Chinese'
// * or whose name begins with the letter 'Wil'.
restaurantCollection.find(
  {
    $or: [{ cuisine: { $nin: ["American ", "Chinese"] } }, { name: /^Wil/ }],
  },
  { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 },
);

// * 23. Find the restaurant Id, name, and grades for those restaurants
// * which achieved a grade of "A"
// * and scored 11 on an ISODate "2014-08-11T00:00:00Z" among many survey dates.
restaurantCollection.find(
  {
    grades: {
      $elemMatch: {
        grade: "A",
        score: 11,
        date: ISODate("2014-08-11T00:00:00Z"),
      },
    },
  },
  { restaurant_id: 1, name: 1, grades: 1, _id: 0 },
); // * here we use $elemMatch to match multiple conditions for an array

// * 24. Find the restaurant Id, name, and grades for those restaurants
// * where the 2nd element of the grades array contains
// * a grade of "A", a score of 9, and an ISODate "2014-08-11T00:00:00Z".
restaurantCollection.find(
  {
    "grades.1.grade": "A",
    "grades.1.score": 9,
    "grades.1.date": ISODate("2014-08-11T00:00:00Z"),
  },
  { restaurant_id: 1, name: 1, grades: 1, _id: 0 },
);

// * 25. Find the restaurant Id, name, address, and geographical location for those restaurants
// * where the 2nd element of the coordinates contains
// * a value more than 42 and up to 52.
restaurantCollection.find(
  {
    "address.coord.1": { $gt: 42, $lte: 52 },
  },
  { restaurant_id: 1, name: 1, address: 1, _id: 0 },
);
// endregion: Q1

// region: Q2
const durhamRestaurantsCollection = db[DURHAM_RESTAURANTS_TABLE];
const durhamForeclosuresCollection = db[DURHAM_FORECLOSURE_TABLE];

// * get the target restaurants, where
// * Rpt_Area_Desc="restaurants" (i.e., Food Service)
// * Seats >= 100
const targetRestaurants = durhamRestaurantsCollection
  .find({
    "fields.rpt_area_desc": "Food Service",
    "fields.seats": { $gte: 100 },
  })
  .toArray();

// * find the min-max longitude and latitude to determine the polygon
let minLongitude = 180;
let maxLongitude = -180;
let minLatitude = 90;
let maxLatitude = -90;
targetRestaurants.forEach((restaurant) => {
  if (!restaurant.geometry) return;
  minLongitude = Math.min(restaurant.geometry.coordinates[0], minLongitude);
  maxLongitude = Math.max(restaurant.geometry.coordinates[0], maxLongitude);
  minLatitude = Math.min(restaurant.geometry.coordinates[1], minLatitude);
  maxLatitude = Math.max(restaurant.geometry.coordinates[1], maxLatitude);
});

// * create a polygon object based on MongoDB's documentation
const targetPolygon = {
  type: "Polygon",
  coordinates: [
    [
      [minLongitude, minLatitude],
      [maxLongitude, minLatitude],
      [maxLongitude, maxLatitude],
      [minLongitude, maxLatitude],
      [minLongitude, minLatitude],
    ],
  ],
};

// * filter and count the foreclosures within our target polygon
const countForeclosures = durhamForeclosuresCollection
  .find({
    geometry: {
      $geoWithin: {
        $geometry: targetPolygon,
      },
    },
  })
  .count();
print(`Number of foreclosures within the target polygon is ${countForeclosures}`);
// endregion: Q2
