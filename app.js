import express from 'express';
import * as http from 'http';
import cors from 'cors';
import { Server } from 'socket.io';
import alert_data from './data.js'
import kafka from './kafka.js';
import * as controller from './controller.js'
const app = express();

app.use(cors())

const httpServer = http.createServer(app);

const io = new Server(httpServer, {
  cors: {
    origin: 'http://localhost:3000',
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
  }
});

kafka.on('message', async function (message) {
  var msg = JSON.parse(message.value)
  if (msg.src_location) {
    controller.storeEvent(msg)
  }
})

io.on('connection', (socket) => {
  const slog = (msg, ...args) => console.log('%s %s ' + msg, Date.now(), socket.id, ...args)
  console.log('a user connected ', socket.id);

  socket.on('disconnect', () => {
    console.log('user disconnected ', socket.id);
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
  kafka.on('message', async function (message) {
    var msg = JSON.parse(message.value)
    if (msg.src_location) {
      io.emit('batch', message.value)
    }
  })
});

controller.getByQuery()

httpServer.listen(3001, () => {
  console.log('Server listening on port 3001');
});

