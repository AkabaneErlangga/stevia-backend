import { MongoClient } from "mongodb";

export function mongo() {
  var uri = "mongodb://localhost:27017";
  const client = new MongoClient(uri);
  return client
}