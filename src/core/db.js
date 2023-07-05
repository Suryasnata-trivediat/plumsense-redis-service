const AWS = require('aws-sdk');
const mysql = require('mysql');
const ssm = new AWS.SSM();
var SSM_USERNAME_PATH = process.env.SSM_USERNAME_PATH;
var SSM_PASSWORD_PATH = process.env.SSM_PASSWORD_PATH;
var SSM_HOST_PATH = process.env.SSM_HOST_PATH;
var SSM_DBNAME_PATH = process.env.SSM_DBNAME_PATH;
var SSM_PORT_PATH = process.env.SSM_PORT_PATH;
var username, password, host, database, port;
var pool;

const hostname = "prod-db-plumsense-instance-1.cru31aoufunh.${self:provider.region}.rds.amazonaws.com";
const dbusername = "admin";
const dbpassword = "Plumsense2023";
const dbport = "3306";
const databasename = "plumsense";

const createPool = ()  => {
  try {
    pool = mysql.createPool({
      host     : hostname,
      user     : dbusername,
      password : dbpassword,
      port     : dbport,
      database: databasename,
      connectionLimit: 100,
      pool: {
        max: 1, 
        min: 0,
        idle: 1000
      }
    });
    return pool;
  } catch(error) {
    console.log("### Error in createPool() ###");
    throw error;
  }
};

const query = async (pool, query, params)  => {
  try {
    // console.log("Inside query----------", pool)
    return new Promise((resolve, reject) => {
      pool.getConnection((error, connection) => {
        if(error) {
          console.log("Error in establishing connection with mysql db", error);
          reject(error);
        }
        connection.query(query, params, (err, response) => {
          connection.release();
          if(err) {
            console.log("Error in quering db", err);
            reject(err);
          }
          resolve(response);
        });
      });
    });
  } catch(error) {
    console.log("### Error in query() in db ###", error);
    throw error;
  }
};

const establishDBConnection = async ()  => {
  try {
    await getParameters(); 
    pool = mysql.createPool({
      host     : host,
      user     : username,
      password : password,
      port     : port,
      database: database,
      pool: {
        max: 1, 
        min: 0,
        idle: 1000
      }
    });
    return new Promise((resolve, reject) => {
      pool.getConnection((error, connection) => {
        if(error) {
          console.log("Error while establishing connection: ", error);
          reject(error);
        } else {
          resolve(connection);
        }
      });
    });
  } catch(error) {
    console.log("### Error in establishDBConnection() ###");
    throw error;
  }
};

const dbname = ()  => {
  return database;
  // return databasename;
};

const queryDB = async (sql, params,connection)  => {
  try {
    return new Promise((resolve, reject) => {
      connection.query(sql, params, (error, result) => {
        if(error) {
          reject(error);
        } else {
          resolve(result);
        }
      });
  });
  } catch(error) {
    console.log("### Error in queryDB() ###");
    throw error;
  }
};

function getParameterValue(parameters, parameterName) {
  var parameterValue;
  parameters.forEach((parameter) => {
    if(parameter.Name === parameterName){
      parameterValue = parameter.Value;
    }
  });
  return parameterValue;
}

const getParameters = async () => {
  try {
    var ssmParameters = ssm.getParameters({
      Names: [SSM_USERNAME_PATH, SSM_PASSWORD_PATH, SSM_PORT_PATH, SSM_HOST_PATH, SSM_DBNAME_PATH],
      WithDecryption: true
    }).promise();
    return new Promise((resolve, reject) => {
      ssmParameters.then(
        (data) => {
          username = getParameterValue(data.Parameters, SSM_USERNAME_PATH);
          password = getParameterValue(data.Parameters, SSM_PASSWORD_PATH);
          host = getParameterValue(data.Parameters, SSM_HOST_PATH);
          database = getParameterValue(data.Parameters, SSM_DBNAME_PATH);
          port = getParameterValue(data.Parameters, SSM_PORT_PATH);
          resolve("Successfully fetched the parameters");
        }  
      ).catch(
        (error) => {
          console.log("Error while fetching parameters: ", error);
          reject(error);
        }  
      );
    });
  } catch(error) {
    console.log("Error in getParameters()", error);
    throw error;
  }
};

function releaseConnection(connection) {
  console.log("Releasing connection - no of open conn-", connection._pool._freeConnections.indexOf(connection))
  return connection._pool._freeConnections.indexOf(connection) != 0 ? connection.destroy() : "";
}

// function releaseConnection(connection) {
//   console.log("Releasing connection - no of open conn-", connection._pool._freeConnections.indexOf(connection))
//   return connection._pool._freeConnections.indexOf(connection) != 0 ? connection.destroy() : "";
// }

// const releasePoolConnections = () => {
//   try {
//     pool.end();
//   } catch(error) {
//     console.log("Error in releasePoolConnections()", error);
//     throw error;
//   }
// };

module.exports = {
  establishDBConnection: establishDBConnection,
  dbname: dbname,
  queryDB: queryDB,
  releaseConnection: releaseConnection,
  createPool: createPool,
  query: query
};