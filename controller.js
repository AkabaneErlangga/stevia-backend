import { mongo } from './mongo.js'

var client = mongo()
var doc = { "seconds": 1666676682, "dst_addr": "192.168.1.121", "dst_ap": "192.168.1.121:853", "dst_port": 853, "eth_dst": "E4:5F:01:4C:0A:A0", "eth_len": 78, "eth_src": "B4:0F:3B:FE:BD:B9", "eth_type": "0x800", "gid": 116, "iface": "eth0", "ip_id": 38768, "ip_len": 44, "msg": "(tcp) experimental TCP options found", "mpls": 0, "pkt_gen": "raw", "pkt_len": 64, "pkt_num": 8272, "priority": 3, "proto": "TCP", "rev": 1, "rule": "116:58:1", "service": "unknown", "sid": 58, "src_addr": "192.168.1.101", "src_ap": "192.168.1.101:51428", "src_port": 51428, "tcp_ack": 0, "tcp_flags": "******S*", "tcp_len": 44, "tcp_seq": 407808445, "tcp_win": 65535, "tos": 0, "ttl": 63, "vlan": 0, "timestamp": "10/25-05:44:42.743333", "dst_country_code": "ID", "dst_country_name": "Indonesia", "dst_location": { "lat": -7.4469, "lon": 112.7181 }, "src_country_code": "US", "src_country_name": "United State", "src_location": { "lat": 38, "lon": -97 } }

const database = client.db('stevia');
const event1s = database.collection('event10s');

const storeEvent = async(event) => {
  const res = await event1s.insertOne(event);
  console.log(`_id: ${res.insertedId}`);
}

const getEvent = async () => {
  const cursor = await event1s.find();
  await cursor.forEach(console.dir);
}
const getByQuery = function (req, res) {
  // const db = req.app.locals.db.mongo.get();
  // let dayLimit = moment(req.query.date, "YYYY-MM-DD");
  let dayLimit = Date.parse("2022-12-20")

  sendAggregate(database, dayLimit).toArray(function (err, event) {
    if (err) {
      // res.send(err);
      console.log(err);
    } 
    else
      // res.json(event);
      console.log(event);
  });
}
const sendAggregate = function (db, dayLimit, timeAmount, timeUnit) {
  let unit = {};
  unit[`$${timeUnit}`] = "$timestamp";
  const granulity = { "$subtract": [
    unit,
    { "$mod": [unit, timeAmount ]}
  ]};
  return event1s.aggregate([
    {
      $match: {timestamp: {$gt: dayLimit}}
    }, {
      $group: {
        _id: {
          interval: {
            "$subtract": [
                { "$subtract": [ "$timestamp", new Date("1970-01-01") ] },
                { "$mod": [
                    { "$subtract": [ "$timestamp", new Date("1970-01-01") ] },
                    1000 * 60 * 15
                ]}
            ]
        },
          time: {"$min": "$timestamp"},
          src_country: "$src_country_iso_code", 
          dest_country: "$dest_country_iso_code", 
          alert_message: "$alert_message"
        },
        count: {
          $sum: 1
        }
      }
    }, {
      $sort: {
        '_id.interval': -1
      }
    }, {
      $limit: 150
    }, {
      $sort: {
        '_id.time': 1
      }
    }
  ],
  {allowDiskUse: true});
}

export {storeEvent, getEvent, getByQuery}