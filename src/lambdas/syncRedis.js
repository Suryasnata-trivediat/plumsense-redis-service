const Redis = require("ioredis");

let redis = undefined;
const redisConnect = () => {
    return new Redis({
      port: process.env.REDIS_PORT,
      host: process.env.REDIS_HOST,
      password: process.env.REDIS_PASSWORD
    });
};

let type, action, id, data, redisKey;

exports.handler =  async (event, context, callback) => {
  try {
    console.log(event);
    
    if(redis == undefined) {
      console.log("Connecting to REDIS.....");
      redis = redisConnect();
      console.log("Connected to REDIS!!");
    } else {
      console.log("Reusing REDIS connection!!");
    }
    
    console.log(`Syncing ${event.Records.length} items`);
    
    if(event && event.Records.length > 0) {
      for (let record of event.Records) {
        var bufferdata = record.kinesis.data;
        let dataToSync = JSON.parse(Buffer.from(bufferdata, 'base64').toString('ascii'));
        console.log("Data to sync :- ", dataToSync);
        
        type = dataToSync.type.toLocaleLowerCase();
        action = dataToSync.action.toLocaleLowerCase();
        id = dataToSync.id.toLocaleLowerCase();
        data = dataToSync.data;
        
        if(type == "transmitter") {
          redisKey = `tr_${id}`;
        } else if(type == "gateway") {
          redisKey = `gtw_${id}`;
        }
        
        await syncData({
          redisKey: redisKey,
          type: type,
          action: action,
          data: data
        });
      }
    }
  } catch(error) {
    console.log(error);
    throw error;
  }
  return "Done";
};

const syncData = async (params)  => {
  try {
    console.log("Before - ", await redis.get(params.redisKey));
    
    switch (`${params.action}`) {
      case 'add':
        
        await redis.set(params.redisKey, JSON.stringify(params.data));
        break;
        
      case 'update':
        
        let cachedData = await redis.get(params.redisKey);
        cachedData = cachedData ? JSON.parse(cachedData) : null;
        
        for (var key of Object.keys(params.data)) {
          cachedData[key] = params.data[key];
        }
        
        await redis.set(params.redisKey, JSON.stringify(cachedData));
        break;
        
      case 'delete':
        await redis.del(params.redisKey);
        break;
        
      default:
        console.log(`No case match found for ${JSON.stringify(params)}`);
    }
    
    console.log("After - ", await redis.get(params.redisKey));
  } catch(error) {
    console.log("### Error in syncData() ###");
    throw error;
  }
};

// let trs = await redis.keys('tr_*');
// let gws = await redis.keys('gtw_*');
// console.log(`TR count - ${trs.length}`);
// console.log(`GW count - ${gws.length}`);