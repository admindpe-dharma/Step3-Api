import {config} from 'dotenv';
config();
import express from "express";
import RollingDoorRoute from "./routes/RollingDoorRoute.js";
import ScannerRoute from "./routes/ScannerRoute.js";
import cors from  "cors";
import ScalesRoute from "./routes/ScalesRoute.js";
import http from 'http';
import { Server } from "socket.io";
import db from "./config/db.js";
import bodyParser from "body-parser";
import { syncEmployeePIDSG, syncPIDSGBin, syncPIDSGContainer, SyncTransaction } from './controllers/Employee.js';
import client, { writePLC } from './Lib/PLCUtility.js';
import { getScales50Kg } from './controllers/Scales.js';
import Queue from 'bull';
import { ExpressAdapter } from '@bull-board/express';
import {createBullBoard} from '@bull-board/api';
import {BullAdapter} from '@bull-board/api/bullAdapter.js';
const app = express();
const server = http.createServer(app);

try {
  await db.authenticate();
  console.log('Database terhubung..');
  
} catch (error) {
  console.log(error);
  
}

const port = 5000;
app.use(cors({
  credentials: false,
  origin: '*'
}));

app.use(bodyParser.json());

const io = new Server(server, {
  cors: {
    origin: "*"
  }
});

const [plcCommandQueue,plcQueue,scaleQueue,pendingQueue,employeeQueue,weightbinQueue]
 = [Queue('PLC Command Queue'),Queue('PLC Connection Queue',{limiter:{max:3,duration:1000}}),Queue('Scale Queue',{limiter:{max:3,duration:1000}}),Queue('Pending Transaction Queue'),Queue('Employee Sync Queue'),Queue('Weightbin Queue')];

 plcCommandQueue.process( async (job,done)=>{
    const res = await writePLC(job.data);
    done(null,res);
 })
plcCommandQueue.on('active',(job,jobres)=>{
    if (!client.isOpen)
      plcQueue.add({id:1});
});
plcQueue.process(  (job,done)=>{
    client.connectRTU(process.env.PORT_PLC, { baudRate: 9600 }).then(x=>
      client.setTimeout(1000)).catch(er=>{
        console.log('plc error');
        plcQueue.add({type:'plc'},{delay:3000});
      }
    );
    done();
});

scaleQueue.process((job,done)=>{
  console.log('scale loading');
	getScales50Kg();
	done();
});
pendingQueue.process(async (job,done)=>{
  const res = await SyncTransaction();
  done(null,res);
});
employeeQueue.process(async(job,done)=>{
  const res = await syncEmployeePIDSG();
  done(null,res);
});
weightbinQueue.process(async(job,done)=>{
  const res1= await syncPIDSGContainer();
  const res2= await syncPIDSGBin();
  done(null,[res1,res2]);
});

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/queues');
const bullBoard = createBullBoard({
  queues: [new BullAdapter(plcCommandQueue),new BullAdapter(scaleQueue),new BullAdapter(plcQueue),new BullAdapter(pendingQueue),new BullAdapter(employeeQueue),new BullAdapter(weightbinQueue)],
  serverAdapter: serverAdapter,
  options:{
    uiConfig:{
      boardTitle:"Step 3 Task Queues"
    }
  }
});
app.use('/queues',serverAdapter.getRouter());
export { Server, io,scaleQueue,plcCommandQueue,employeeQueue,weightbinQueue,pendingQueue,plcQueue };

app.use(RollingDoorRoute);
app.use(ScannerRoute);
app.use(ScalesRoute);


 io.on('connection', (socket) => {
  console.log('Client connected');
  
  socket.on('disconnect', () => {
    console.log('Client disconnected');
  });
}); 

server.listen(port, () => {
  pendingQueue.add({id:1});
  employeeQueue.add({id:2});
  weightbinQueue.add({id:3});
  scaleQueue.add({type:'scale'});
  plcQueue.add({type:'plc'});
  console.log(`Server up and running on port ${port}`);
});
console.log("check max weight");
//checkMaxWeight();