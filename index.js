"use strict";

const http = require(process.env.protocol);
const AWS = require("aws-sdk");
const crypto = require("crypto");


const s3 = new AWS.S3();
const HOSTNAME = process.env.hostname;
const PORT = process.env.port;
const PATHGET  = process.env.path_getDocument;
const PATHPATCH = process.env.path_patchDocument;
const STAGINGBUCKET = process.env.staging_bucket;

let jsonDocument = {
  documentKey: "",
  documentState: "",
};
exports.handler = async (event) => {
  console.log(event);
  let bucketName;
  console.log("Buket Name: " + event.Records[0].s3.bucket.name);
  console.log("Object Key: " + event.Records[0].s3.object.key);
  console.log("Object Size: " + event.Records[0].s3.object.size);
  let params = {
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
      if(bucketName === STAGINGBUCKET){
        jsonDocument.documentState = "staged";
      }else{
        jsonDocument.contentLenght = event.Records[0].s3.object.size;
        jsonDocument.documentState = "available";
        params.Bucket = bucketName;
        params.Key = jsonDocument.documentKey;
        const { Body } = await s3.getObject(params).promise();
        console.log(Body);
        let doc = await getDocumentFromDB(jsonDocument.documentKey);
        console.log(doc);
        jsonDocument.checkSum = crypto.createHash(doc.documentType.checksum).update(Body).digest("hex"); 
      }
      console.log(jsonDocument);
      break;
    case "ObjectRestore:Completed":
      jsonDocument.documentState = "available";
      break;
    case "LifecycleTransition":
      jsonDocument.documentState = "freezed";
      break;
    case "ObjectRestore:Delete":
      jsonDocument.documentState = "freezed";
      break;
    case "LifecycleExpiration:Delete":
      jsonDocument.documentState = "deleted";
      break;
    case "ObjectRemoved:Delete":
      jsonDocument.documentState = "deleted";
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
function getDocumentFromDB(docKey) {
  const options = {
    method: "GET",
    hostname: HOSTNAME,
    port: PORT,
    path: PATHGET + "/" + docKey,
    headers: {
      "Content-Type": "application/json",
    }, 
  };
  return new Promise((resolve, reject) => {
    const req = http.request(options, (res) => {
      
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
    req.end();
  });
}
function updateDynamo(data) {
  const options = {
    method: "GET",
    hostname: HOSTNAME,
    port: PORT,
    path: PATHPATCH + "/" + docKey,
    headers: {
      "Content-Type": "application/json",
    }, 
  };
  return new Promise((resolve, reject) => {
    const req = http.requesst(options, (res) => {
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
