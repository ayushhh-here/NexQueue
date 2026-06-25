const express = require("express");
const {Queue, QueueEvents} = require("bullmq");
const app = express();
const port = 5000;

app.use(express.json());


// Queue
const verifyUser = new Queue("user-verification-queue");
const VerificationQueueEvents = new QueueEvents("user-verification-queue");
const mailQueue = new Queue("mail-queue");


const checkUserVerification= (jobId) => {
    return new Promise((resolve,reject) => {
        VerificationQueueEvents.on("completed",({jobId : completedjobId, returnvalue})=>{
            if(jobId === completedjobId){
                const {isValidUser, rest} = returnvalue;
                resolve({isValidUser, rest});
            }
        });
        VerificationQueueEvents.on("failed", ({jobId: failedjobId , failedReason})=>{
            if(jobId === failedjobId){
                reject(new Error(failedReason));
            }
        });
    });
};
app.post("/order",async(req,res) =>{
    try{
    const { orderId, productName, price, userId} = req.body;

    const job = await verifyUser.add("Verify User",{userId});
    let {isValidUser , rest} =  await checkUserVerification(job.id);
        
    if(!isValidUser){
        return res.send({
            message: "User is not valid"
       })
    }

    // Save order to database

     const mailJob = await mailQueue.add("Send Mail",{
        from: "apniCompany@company.com",
        to: rest.email,
        subject: "Thank you for shopping",
        body: "Success placong of order"
     });

    res.send({
        message: "User is valid",
        mailJob: mailJob.id,
        rest
    });



    } catch (error) {
        console.log({err});
    }
});

app.listen(port,() => console.log("Order Server started at port 5000"));