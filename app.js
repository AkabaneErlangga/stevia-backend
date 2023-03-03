import express from "express";
import * as http from "http";
import cors from "cors";
import { Server } from "socket.io";
import alert_data from "./data.js";
import kafka from "./kafka.js";
import * as controller from "./controller.js";
const app = express();

app.use(cors());

const httpServer = http.createServer(app);
let msgs = [];
let lmsg;

const io = new Server(httpServer, {
  cors: {
    origin: "*",
    methods: "GET,HEAD,PUT,PATCH,POST,DELETE",
  },
});

kafka.on('message', async function (message) {
  var msg = JSON.parse(message.value)
  if (msg.src_location && msg.dst_location && msg.src_country_code && msg.dst_country_code && msg['@timestamp']) {
    msg['@timestamp'] = new Date(msg['@timestamp'])
    controller.storeEvent(msg)
  }
})

io.on("connection", (socket) => {
  const slog = (msg, ...args) =>
    console.log("%s %s " + msg, Date.now(), socket.id, ...args);
  console.log("a user connected ", socket.id);

  socket.on("disconnect", () => {
    console.log("user disconnected ", socket.id);
    // consumer.close(true, function(err, message) {
    //   console.log("consumer has been closed..");
    // });
  });

  // function sendBatch(i, max, delay) {
  //   // if (i >= max) return slog('Done batch %s %s', max, delay)
  //   if (i >= max) {
  //     i = 1
  //   }
  //   io.emit('batch', alert_data[i])
  //   setTimeout(() =>
  //     sendBatch(i + 1, max, delay)
  //     , delay)
  // }
  // sendBatch(1, 26, 3000)
  kafka.on("message", async function (message) {
    var msg = JSON.parse(message.value);
    if (
      msg.src_location &&
      msg.dst_location &&
      msg.src_country_code &&
      msg.dst_country_code
    ) {
      if (
        lmsg == null ||
        (lmsg.src_location != msg.src_location &&
          lmsg.dst_location != msg.dst_location &&
          lmsg.msg != msg.msg)
      ) {
        lmsg = msg;
        msgs.push(lmsg);
      }
      if (msgs.length > 5) {
        msgs.splice(0, 1);
      }
      io.emit("batch", msgs);
      // console.log(msg);
    }
  });

  socket.on("getByQuery", (req, res) => {
    //log req query
    console.log(req.startDate);
    let start = new Date(req.startDate);
    let end = new Date(req.endDate);
    let loop = start;
    controller.getEvent(start, end).then(async (data) => {
      let lastIndex = 0;
      let batch = []
      console.log(data);
      while (loop <= end) {
        while (
          data[lastIndex] && new Date(data[lastIndex]["@timestamp"]).getTime() === loop.getTime()
        ) {
          batch.push(data[lastIndex])
          if (batch.length > 5) {
            batch.splice(0, 1);
          }
          io.emit('history', batch);
          // console.log(data[lastIndex]);
          lastIndex++;
        }
        let newDate = loop.setSeconds(loop.getSeconds() + 1);
        loop = new Date(newDate);
        await controller.sleep(1000);
      }
    });
  });
});

httpServer.listen(3001, "0.0.0.0", () => {
  console.log("Server listening on port 3001");
});
