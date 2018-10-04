var express = require("express");
var path = require("path");
var bodyParser = require("body-parser");
var app = express();
var libname = "/home/ding/jaguar/lib/jaguarnode";
const Jag = require( libname );
var jaguar = Jag.JaguarAPI();
// console.log(jaguar.connect("192.168.7.120", 8899, "admin", "jaguarjaguarjaguar", "test"));


app.use(express.static(path.join(__dirname,"/html")));
app.use(bodyParser.json());

app.post('/signin', function (req, res) {
  var user_name=req.body.email;
  var password=req.body.password;
  if(jaguar.connect("192.168.7.120", 8899, user_name, password, "test") == 1){
  	res.send('success');
  }
  else{
  	res.send('Failure');
  }
})

var server = app.listen(8880, function () {
    var host = '192.168.7.120';
    var port = 8880;
    console.log("应用实例，访问地址为 http://%s:%s", host, port)
})
