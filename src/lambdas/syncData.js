const db = require('../core/db');
const AWS = require('aws-sdk');
let kinesis = new AWS.Kinesis();

let connection = undefined;
const connAsPromise = db.establishDBConnection();

exports.handler =  async (event, context, callback) => {
  try {
    console.log(event);
    
    if(connection == undefined) {
      connection =  await connAsPromise;
    }
    
    let sql = `SELECT * FROM ${db.dbname()}.gateway`;
      
    let trs = await db.queryDB(sql, 0 , connection);
    console.log(trs.length);
    
    
    let records = [];
    for (var tr of trs) {
      let id = tr.id;
      let data = {
        type: "gateway",
        action: "add",
        id: id,
        data: tr
      };
      records.push({
        Data: JSON.stringify(data),
        PartitionKey: "1"
      });
    }
    
    console.log("records.length ----", records.length);
    
    await putRecords({
      Records: records,
      StreamName: "redis-service-beta-test-SyncRedisStream"
    });
    
  } catch(error) {
    console.log(error);
  }
  return "Done";
};

const putRecords = async (params)  => {
  try {
    return new Promise((resolve, reject) => {
      kinesis.putRecords(params, (error, result) => {
        if(error) {
          console.log("Error ---------- ", error);
          reject(error);
        } else {
          resolve(result);
        }
      });
    });
  } catch(error) {
    console.log("### Error in putRecord() ###");
    throw error;
  }
};