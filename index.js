var redis = require("redis").createClient();


redis.subscribe("article");

redis.on("subscribe", function (channel, count) {
    console.log("Subscribed to "+channel);
});

redis.on("message", function(channel, message){
    if(channel=="article"){
        console.log("\nINCOMING ARTICLE");
        try{
            var data = JSON.parse(message);
        }catch(E){
            data = {};
        }
        console.log(data.title);
        console.log(new Array((data.title || "").length+1).join("="))
        console.log(data.url);
        console.log((data.content || "").substr(0,256)+" ...");
        console.log(new Date());
    }
});