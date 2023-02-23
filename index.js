"use strict";
const https = require("https");
const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const HOSTNAME = process.env.HOST_NAME || "localhost";

var jsonDocument = {
  documentKey: "",
  documentState: "",
  checkSum: "",
  contentLenght: 0,
};
/**
* Event Handler
*
* @param {*} event
* @returns
*/
exports.handler = async (event) => {
  console.log(event);
  var bucketName;
  console.log("Buket Name: " + event.Records[0].s3.object.name);
  console.log("Object Key: " + event.Records[0].s3.object.key);
  console.log("Object Size: " + event.Records[0].s3.object.size);
  var params = {
    Bucket: "",
    Key: "",
  };
  //event.Records.forEach((record) => {
  bucketName = event.Records[0].s3.bucket.name;
  jsonDocument.documentKey = event.Records[0].s3.object.key;
  switch (event.Records[0].eventName) {
    case "ObjectCreated:*":
      break;
    case "ObjectCreated:Put":
      jsonDocument.contentLenght = event.Records[0].s3.object.size;
      jsonDocument.documentState = "AVAILABLE";
      params.Bucket = bucketName;
      params.Key = jsonDocument.documentKey;
      const { Body } = await s3.getObject(params).promise();
      console.log(Body);
      var doc = await getDocumentFromDB(jsonDocument.documentKey);
      console.log(doc);
      jsonDocument.checkSum = hashDocument(Body, doc.documentType);
      console.log(jsonDocument);
      break;
    case "ObjectRestore:Completed":
      jsonDocument.documentState = "AVAILABLE";
      break;
    case "LifecycleTransition":
      jsonDocument.documentState = "FREEZED";
      break;
    case "ObjectRestore:Delete":
      jsonDocument.documentState = "FREEZED";
      break;
    case "LifecycleExpiration:Delete":
      jsonDocument.documentState = "DELETED";
      break;
    case "ObjectRemoved:Delete":
      jsonDocument.documentState = "DELETED";
      break;
    default:
      const response = {
        statusCode: 200,
      };
    return response;
  }
  const res = await updateDynamo(jsonDocument);
  console.log(res);
  console.log("############## EXIT  ####################");
  const response = {
    statusCode: 200,
  };
  return response;
};
/**
* Do a request with options not provided.
*
* @param {Object} options
* @param {Object} data
* @return {Promise} a promise of request
*/
function getDocumentFromDB(docKey) {
  const options = {
    method: "GET",
    hostname: HOSTNAME,
    path: "/safestorage/internal/v1/documents/" + docKey,
    headers: {
      "Content-Type": "application/json",
    },
  };
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      // res.setEncoding("utf8");
      let responseBody = "";

      res.on("data", (chunk) => {
        responseBody += chunk;
      });

      res.on("end", () => {
        resolve(JSON.parse(responseBody));
      });
    });

    req.on("error", (err) => {
      console.error(err);
      reject(err);
    });

    // req.write(data);
    req.end();
  });
}

var crypto = require("crypto");

const hashDocument = (document, docType) => {
  var hash;
  if (docType == "PN_AAR") {
    hash = crypto.createHash("md5").update(document).digest("hex");
  } else {
    hash = crypto.createHash("sha256").update(document).digest("hex");
  }

  return hash;
};

/**
* TO DO
*/
function updateDynamo(data) {
  const options = {
    method: "PATCH",
    hostname: HOSTNAME,
    path: "/safestorage/internal/v1/documents/" + data.documentKey,
    headers: {
      "Content-Type": "application/json",
    },
  };
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      // res.setEncoding("utf8");
      let responseBody = "";

      res.on("data", (chunk) => {
        responseBody += chunk;
      });

      res.on("end", () => {
        resolve(JSON.parse(responseBody));
      });
    });

    req.on("error", (err) => {
      console.error(err);
      reject(err);
    });

    req.write(data);
    req.end();
  });
}