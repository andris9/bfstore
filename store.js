// handle parent death
(function(){
    var FFI = require('node-ffi');
    var current = new FFI.Library(null, {"prctl": ["int32", ["int32", "uint32"]]})
    
    //1: PR_SET_PDEATHSIG, 15: SIGTERM
    var returned = current.prctl(1,15);
    
    process.on('SIGTERM',function(){
            //do something interesting
            process.exit(1);
    });
})();

var redis = require("redis").createClient(),
    fetchUrl = require("fetch").fetchUrl,
    config = require("./config/config.json"),
    crypto = require("crypto");

redis.subscribe("article");

redis.on("subscribe", function (channel, count) {
    console.log("Subscribed to "+channel);
});

redis.on("message", function(channel, message){
    if(channel=="article"){
        try{
            var article = JSON.parse(message);
        }catch(E){
            article = {};
            console.log(new Date(), "Parse failed");
            return;
        }
        
        console.log(new Date(), "Received: "+(article.title || ""));
        sendArticle(article, function(err, status){
            if(err){
                console.log("Save failed: "+(err.message || err));
            }else{
                console.log("Saved as " + md5(article.url));
            }
        })
        
        /*
        console.log(article);
        
        console.log(article.title);
        console.log(new Array((article.title || "").length+1).join("="))
        console.log(article.url);
        console.log((article.content || "").substr(0,256)+" ...");
        console.log(new Date());
        **/
    }
});


function sendArticle(article, callback){
    var url = "http://"+config.searchServer+":9200/"+config.database+"/article/"+md5(article.url);
    fetchUrl(
        url,
        {
            method: "PUT",
            outputEncoding: "utf-8",
            payload: JSON.stringify(article)
        },
    
        function(err, meta, response){
            if(err){
                callback(err);
            }else{
                callback(null, true);
            }
        });
}

function md5(str){
    return crypto.createHash("md5").update(str).digest("hex");
}