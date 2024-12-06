/** @format */

const fs = require("fs");
const { MongoClient } = require("mongodb");

// Constants
const DB_USER = "root";
const DB_PASSWORD = "super_duper_password";
const DB_NAME = "big_data_hw4";
const MONGO_URI = `mongodb://${DB_USER}:${DB_PASSWORD}@localhost:27017/${DB_NAME}?authSource=admin`;
const RESTAURANT_TABLE = "restaurants";
const DURHAM_RESTAURANTS_TABLE = "durham_restaurants";
const DURHAM_FORECLOSURE_TABLE = "durham_nc_foreclosure";

async function processQ1() {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const restaurantCollection = db.collection(RESTAURANT_TABLE);

    // Results object
    let results = {};

    // Q1.1: Count the number of documents in the restaurants collection
    results["Q1.1"] = await restaurantCollection.countDocuments({});

    // Q1.2: Display all the documents in the collection
    results["Q1.2"] = await restaurantCollection.find({}).toArray();

    // Q1.3: Display restaurant_id, name, borough, and cuisine for all documents
    results["Q1.3"] = await restaurantCollection
      .find(
        {},
        {
          projection: { restaurant_id: 1, name: 1, borough: 1, cuisine: 1 },
        },
      )
      .toArray();

    // Q1.4: Display without the _id field
    results["Q1.4"] = await restaurantCollection
      .find(
        {},
        {
          projection: { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 },
        },
      )
      .toArray();

    // Q1.5: Display restaurant_id, name, borough and zip code, exclude the field _id
    results["Q1.5"] = await restaurantCollection
      .find(
        {},
        {
          projection: { restaurant_id: 1, name: 1, borough: 1, "address.zipcode": 1, _id: 0 },
        },
      )
      .toArray();

    // Q1.6: Display all the restaurants in the Bronx
    results["Q1.6"] = await restaurantCollection.find({ borough: "Bronx" }).toArray();

    // Q1.7: Display the first 5 restaurants in the Bronx
    results["Q1.7"] = await restaurantCollection.find({ borough: "Bronx" }).limit(5).toArray();

    // Q1.8: Display the second 5 restaurants in the Bronx (skip the first 5)
    results["Q1.8"] = await restaurantCollection.find({ borough: "Bronx" }).skip(5).limit(5).toArray();

    // Q1.9: Find the restaurants with any score more than 85
    results["Q1.9"] = await restaurantCollection.find({ "grades.score": { $gt: 85 } }).toArray();

    // Q1.10: Find the restaurants that achieved score more than 80 but less than 100
    results["Q1.10"] = await restaurantCollection.find({ "grades.score": { $gt: 80, $lt: 100 } }).toArray();

    // Q1.11: Find the restaurants which locate in longitude less than -95.754168
    results["Q1.11"] = await restaurantCollection.find({ "address.coord.0": { $lt: -95.754168 } }).toArray();

    // Q1.12 and Q1.13: Complex conditions using $and and equivalent implicit and conditions
    results["Q1.12"] = await restaurantCollection
      .find({
        $and: [
          { cuisine: { $ne: "American " } },
          { "grades.score": { $gt: 70 } },
          { "address.coord.0": { $lt: -65.754168 } },
        ],
      })
      .toArray();

    results["Q1.13"] = await restaurantCollection
      .find({
        cuisine: { $ne: "American " },
        "grades.score": { $gt: 70 },
        "address.coord.0": { $lt: -65.754168 },
      })
      .toArray();

    // Q1.14: Find and sort by cuisine descending
    results["Q1.14"] = await restaurantCollection
      .find({
        cuisine: { $ne: "American " },
        "grades.grade": "A",
        borough: { $ne: "Brooklyn" },
      })
      .sort({ cuisine: -1 })
      .toArray();

    // Q1.15 to Q1.22: Various filters on restaurant names and cuisines
    results["Q1.15"] = await restaurantCollection
      .find(
        { name: /^Wil/ },
        {
          projection: { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 },
        },
      )
      .toArray();

    results["Q1.16"] = await restaurantCollection
      .find(
        { name: /ces$/ },
        {
          projection: { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 },
        },
      )
      .toArray();

    results["Q1.17"] = await restaurantCollection
      .find(
        { name: /Reg/ },
        {
          projection: { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 },
        },
      )
      .toArray();

    results["Q1.18"] = await restaurantCollection
      .find({
        borough: "Bronx",
        $or: [{ cuisine: "American " }, { cuisine: "Chinese" }],
      })
      .toArray();

    results["Q1.19"] = await restaurantCollection
      .find(
        {
          borough: { $in: ["Staten Island", "Queens", "Bronx", "Brooklyn"] },
        },
        {
          projection: { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 },
        },
      )
      .toArray();

    results["Q1.20"] = await restaurantCollection
      .find(
        {
          borough: { $nin: ["Staten Island", "Queens", "Bronx", "Brooklyn"] },
        },
        {
          projection: { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 },
        },
      )
      .toArray();

    results["Q1.21"] = await restaurantCollection
      .find(
        {
          "grades.score": { $lt: 10 },
        },
        {
          projection: { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 },
        },
      )
      .toArray();

    results["Q1.22"] = await restaurantCollection
      .find(
        {
          $or: [{ cuisine: { $nin: ["American ", "Chinese"] } }, { name: /^Wil/ }],
        },
        {
          projection: { restaurant_id: 1, name: 1, borough: 1, cuisine: 1, _id: 0 },
        },
      )
      .toArray();

    results["Q1.23"] = await restaurantCollection
      .find(
        {
          grades: {
            $elemMatch: {
              grade: "A",
              score: 11,
              date: new Date("2014-08-11T00:00:00Z"),
            },
          },
        },
        {
          projection: { restaurant_id: 1, name: 1, grades: 1, _id: 0 },
        },
      )
      .toArray();

    results["Q1.24"] = await restaurantCollection
      .find(
        {
          "grades.1.grade": "A",
          "grades.1.score": 9,
          "grades.1.date": new Date("2014-08-11T00:00:00Z"),
        },
        {
          projection: { restaurant_id: 1, name: 1, grades: 1, _id: 0 },
        },
      )
      .toArray();

    results["Q1.25"] = await restaurantCollection
      .find(
        {
          "address.coord.1": { $gt: 42, $lte: 52 },
        },
        {
          projection: { restaurant_id: 1, name: 1, address: 1, _id: 0 },
        },
      )
      .toArray();

    return results;
  } catch (err) {
    console.error("Error running queries:", err);
  } finally {
    await client.close();
  }
}
async function processQ2(results) {
  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const durhamRestaurantsCollection = db.collection(DURHAM_RESTAURANTS_TABLE);
    const durhamForeclosuresCollection = db.collection(DURHAM_FORECLOSURE_TABLE);

    // Q2: Queries related to Durham restaurants and foreclosures
    const targetRestaurants = await durhamRestaurantsCollection
      .find({
        "fields.rpt_area_desc": "Food Service",
        "fields.seats": { $gte: 100 },
      })
      .toArray();

    let minLongitude = 180,
      maxLongitude = -180,
      minLatitude = 90,
      maxLatitude = -90;
    targetRestaurants.forEach((restaurant) => {
      if (!restaurant.geometry) return;
      const coords = restaurant.geometry.coordinates;
      minLongitude = Math.min(coords[0], minLongitude);
      maxLongitude = Math.max(coords[0], maxLongitude);
      minLatitude = Math.min(coords[1], minLatitude);
      maxLatitude = Math.max(coords[1], maxLatitude);
    });

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

    const countForeclosures = await durhamForeclosuresCollection.countDocuments({
      geometry: {
        $geoWithin: {
          $geometry: targetPolygon,
        },
      },
    });

    results["Q2"] = countForeclosures;

    return results;
  } catch (err) {
    console.error("Error running queries:", err);
  } finally {
    await client.close();
  }
}

console.log("Start processing");
processQ1().then((results) => {
  console.log("Finished Q1");
  processQ2(results).then((finalResults) => {
    console.log("Finished Q2, writing results");
    const filename = "./results.json";
    fs.writeFile(filename, JSON.stringify(finalResults), (err) => {
      if (err) {
        console.error("Error saving JSON to file:", err);
      } else {
        console.log(`Results saved to ${filename}`);
      }
    });
    console.log("Done");
  });
});
