const express = require("express");
const {Queue, Worker} = require("bullmq");
const app = express();
const port = 5001;

app.use(express.json());

const userDb =[{
    id:1,
    name: "Rahul Nikam",
    password: "123456",
    email: "rahul@gmail.com"
}];
const verificationWorker = new Worker("user-verification-queue",(job)=>{
    const userId = job.data.userId;
    console.log(`job recieved with userId as ${userId} amd ${job.id}`);

    const isValidUser = userDb.some((item)=> item.id === userId);
    console.log(`user valid ${isValidUser}`);
    const {password, ...rest} = userDb[0];
    return {isValidUser, rest};
},

{connection:{
    host: "127.0.0.1",
    port: 6379
} });
app.listen(port,() => console.log("User Server started at port 5001"));