var fs = require("fs"),
    fetch = require("fetch"),
    config = JSON.parse(fs.readFileSync(__dirname + "/config.json", "utf-8")),
    mapping = JSON.parse(fs.readFileSync(__dirname + "/mapping.json", "utf-8"));


console.log("start")
createIndex(function(err){
    if(err){
        console.log("Index failed :S");
    }else{
        console.log("Index created");
        setupMapping(function(err){
            if(err){
                console.log("Mapping failed :S");
            }else{
                console.log("Mapping created");
            }
        });
    }
});


function createIndex(callback){
    var url = "http://"+config.searchServer+":9200/"+config.database+"/";
    fetch.fetchUrl(
        url,
        {
            method: "PUT",
            outputEncoding: "utf-8"
        },
    
        function(err, meta, response){
            if(err){
                callback(err);
            }else{
                callback(null, true);
            }
        });
}

function setupMapping(callback){
    var url = "http://"+config.searchServer+":9200/"+config.database+"/article/_mapping";
    fetch.fetchUrl(
        url,
        {
            method: "PUT",
            payload: JSON.stringify(mapping),
            outputEncoding: "utf-8"
        },
    
        function(err, meta, response){
            if(err){
                callback(err);
            }else{
                callback(null, true);
            }
        });
}
