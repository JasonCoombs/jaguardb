import express from 'express';
import bodyParser from 'body-parser';
import jwt from 'jsonwebtoken';
import config from './config'

let app = express();

app.use(bodyParser.json());


app.get("/",(req,res) =>{
    res.send("ok")
})

// app.post("/api/users",(req, res) =>{
//     console.log(req.body);
//     let error = {"error":"failed"};
//     res.status(400).json(error);
// })

app.post("/api/login",(req, res) =>{
    console.log(req.body);
    if(req.body.username == "admin" && req.body.password == "admin"){
        const token = jwt.sign({
            username:req.body.username
        },config.jwtSecret);
        res.json({token})
    }else(
        res.send(400).json({"error":"Invalid username or password"})
    );

    // let error = {"error":"failed"};
    // res.body = "hello weiyi";
})

app.listen(6060, ()=>console.log('running'));
