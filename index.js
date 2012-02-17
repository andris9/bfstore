var redis = require("redis").createClient();


redis.subscribe("article");

redis.on("message", function(channel, message){
    if(channel=="article"){
        console.log("INCOMING ARTICLE");
        try{
            var data = JSON.parse(message);
        }catch(E){
            data = {};
        }
        console.log(article.title);
        console.log(article.url);
        console.log((article.article || "").substr(0,256)+" ...");
    }
});