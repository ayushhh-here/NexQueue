const express = require("express");
const {Worker} = require("bullmq");

const app = express();
const port = 5002;

app.use(express.json());
const mailWorker = new Worker("mail-queue", async (job) =>{
    const {from,to,subject,body}=job.data;
     const sendMailToUser = await sendMail(from, to , subject , body );
   
},{
    connection: {
        host: "127.0.0.1",
        port: 6379
    }
});

const sendMail = async (from, to, subject, body) => {
    console.log(`Mail was sent on ${to}`);
};
app.listen(port,() => console.log("Mail Server started at port 5002"));