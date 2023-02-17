'use strict';

const https = require('https');
const AWS = require('aws-sdk')
const s3 = new AWS.S3()


const HOSTNAME = process.env.HOST_NAME || "localhost";

var jsonDocument = JSON.stringify({
  documentKey: "",
  documentState: "",
  checkSum: "",
  contentLenght: 0
})


/**
 * Event Handler
 * 
 * @param {*} event 
 * @returns 
 */
exports.handler = async (event) => {
  var bucketName;

  var params = {
    Bucket: "",
    Key: ""
  };

  event.Records.forEach(record => {
    bucketName = record.s3.bucket.name;
    jsonDocument.documentKey = record.s3.object.key;
    jsonDocument.contentLenght = record.s3.object.size;

    if (record.eventName == "ObjectCreated:*" || record.eventName == "ObjectRestore:Completed") {
      jsonDocument.documentState = "AVAILABLE";
    }
    if (record.eventName == "LifecycleTransition" || record.eventName == "ObjectRestore:Delete") {
      jsonDocument.documentState = "FREEZED";
    }
    if (record.eventName == "LifecycleExpiration:Delete" || evenrecord.eventNameName == "ObjectRemoved:Delete") {
      jsonDocument.documentState = "DELETED";
    }

    params.Bucket = bucketName;
    params.Key = jsonDocument.documentKey;
    // Get S3 file from source and transfer to target.
    s3.getObject(params, (err, data) => {
      if (err) {
        console.error(err);
      } else {
        var doc = getDocumentFromDB(jsonDocument.documentKey);
        
        jsonDocument.checkSum = hashDocument(data.Body, doc.documentType);
         
        const res = updateDynamo(jsonDocument);
        
      } 
    })
  });

  const response = {
    statusCode: 200
  };

  return response;

}

const getDocumentFromDB = (docKey) => {

}

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
      'Content-Type': 'application/json',
    }
  }
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
      reject(err);
    });

    // req.write(data);
    req.end();
  });
}

var crypto = require('crypto');


const hashDocument = (document, docType) => {
  var hash;
  if (docType == "PN_AAR") {
    hash = crypto.createHash('md5').update(document).digest('hex');
  } else {
    hash = crypto.createHash('sha256').update(document).digest('hex');
  }

  return hash;
}

/**
 * TO DO
 */
c
function updateDynamo(data) {
  const options = {
    method: "PATCH",
    hostname: HOSTNAME,
    path: "/safestorage/internal/v1/documents/" + data.documentKey,
    headers: {
      'Content-Type': 'application/json',
    }
  }
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
      reject(err);
    });

    req.write(data);
    req.end();
  });
}