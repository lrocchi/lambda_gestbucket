"use strict";
import { SendMessageCommand } from "@aws-sdk/client-sqs";
const http = require(process.env.PnSsGestoreRepositoryProtocol);
const AWS = require("aws-sdk");
const crypto = require("crypto");

const sqs = new AWS.SQS();
const s3 = new AWS.S3();

const HOSTNAME = process.env.PnSsHostname;
const PORT = process.env.PnSsGestoreRepositoryPort;
const PATHGET = process.env.PnSsGestoreRepositoryPathGetDocument;
const PATHPATCH = process.env.PnSsGestoreRepositoryPathPatchDocument;
const STAGINGBUCKET = process.env.PnSsStagingBucketName;

exports.handler = async (event) => {
  let jsonDocument = {
    documentKey: "",
    documentState: "",
  };

  console.log(JSON.stringify(event));
  let bucketName;
  console.log("Buket Name: " + event.Records[0].s3.bucket.name);
  console.log("Object Key: " + event.Records[0].s3.object.key);
  console.log("Object Size: " + event.Records[0].s3.object.size);
  let params = {
    Bucket: "",
    Key: "",
  };
  bucketName = event.Records[0].s3.bucket.name;
  jsonDocument.documentKey = event.Records[0].s3.object.key;
  switch (event.Records[0].eventName) {
    case "ObjectCreated:*":
      break;
    case "ObjectCreated:Put":
      if (bucketName === STAGINGBUCKET) {
        jsonDocument.documentState = "staged";
      } else {
        jsonDocument.contentLenght = event.Records[0].s3.object.size;
        jsonDocument.documentState = "available";
        params.Bucket = bucketName;
        params.Key = jsonDocument.documentKey;
        const { Body } = await s3.getObject(params).promise();
        console.log(Body);
        const doc = await getDocumentFromDB(jsonDocument.documentKey);
        console.log(doc);
        console.log(doc.document);
        console.log(doc.document.documentType.checksum);
        console.log(JSON.stringify(doc.document.documentType.checksum));
        jsonDocument.checkSum = crypto
          .createHash(doc.document.documentType.checksum)
          .update(Body)
          .digest("hex");
      }
      console.log(jsonDocument);
      break;
    case "ObjectCreated:Copy":
      jsonDocument.documentState = "available";
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
  const res = await updateDynamo(jsonDocument, event);
  console.log(res);
  console.log("############## EXIT  ####################");
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
function updateDynamo(data, eventData, retries = 3, backoff = 2000) {
  const options = {
    method: "PATCH",
    hostname: HOSTNAME,
    port: PORT,
    path: PATHPATCH + "/" + data.documentKey,
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
        switch (res.statusCode) {
          case 404:
            reject;
            break;
          case 500:
            if (retries > 0) {
              setTimeout(() => {
                req.destroy();
                return updateDynamo(data, eventData, retries - 1, backoff * 2);
              }, backoff);
            } else {
              console.log("Retry attempts terminated");
              sendOnQueue(eventData);
            }
            break;
          default:
            resolve(JSON.parse(responseBody));
            break;
        }
      });
    });
    req.on("error", (err) => {
      console.error(err);
      reject(err);
    });
    req.write(JSON.stringify(data));
    req.end();
  });
}

function sendOnQueue(msg) {
  const params = {
    DelaySeconds: 10,
    MessageAttributes: {
      Author: {
        DataType: "String",
        StringValue: "Preeti",
      },
    },
    MessageBody: msg,
    QueueUrl:
      "https://sqs.us-east-1.amazonaws.com/672607396920/lambda-sqs-demo-queue",
  };

  sqs.sendMessage(params, (err, data) => {
    if (err) {
      console.err("Error", err);
    } else {
      console.log("Success", data.MessageId);
    }
  });
}
