var fetch = require("fetch"),
    Gearman = require("node-gearman"),
    urllib = require("url"),
    zlib = require("zlib"),
    utillib = require("util"),
    config = require("./config/config.json"),
    queryParser = require("./queryparser"),
    gearman,
    
    gearmanHost = config.gearmanHost,
    searchServerHost = config.searchServer,
    
    searchServerPort = 9200;

gearman = new Gearman(gearmanHost);

request = {queryString:"vanamehed"}

console.log("Starting search gearman...")
gearman.registerWorker("search", function(payload, worker){
    console.log(1)
    if(!payload){
        worker.error();
        return;
    }
    
    console.log(payload.toString("utf-8"))
    
    var request;
    try{
        request = JSON.parse((payload || "").toString("utf-8"));
        console.log(2)
    }catch(E){
        console.log(E)
        worker.error();
        return;
    }
    
    console.log("Search query: "+ payload.toString("utf-8"));
    runQuery(request, function(err, data){
        if(err){
            worker.error();
            return;
        }
        console.log((data && data.results && data.results.length || 0)+" results");
        worker.end(JSON.stringify(data));
    });
});


function runQuery(options, callback){
    checkAllowedKeys(options, function(err, options){
        if(err){
            return callback(err);
        }
        
        buildQuery(options, function(err, options, query, querystr){
            if(err){
                return callback(err);
            }
            
            makeQuery(query, function(err, resultset){
                var results = [],
                    output = {};
                if(err){
                    return callback(err);
                }
                
                output.query = options;
                
                if(resultset && resultset.hits && resultset.hits.hits){
                    results = resultset.hits.hits;
                    output.totalResults = parseInt(resultset.hits.total,10) || 0;
                    output.results = [];
                }
                
                results.forEach(function(result){
                    var item = {
                            "id": result._id
                        },
                        fields = result && result.fields || {},
                        highlight = result && result.highlight || {};
                    
                    item.title = fields.title;
                    item.hlTitle = [].concat(highlight.title || fields.title || "").join(" ... ");
                    item.hlContents = [].concat(highlight.contents || fields.contents || "").join(" ... ");
                    item.html = fields.html || "";
                    item.domain = fields.domain;
                    item.time = fields.time; //new Date(fields.time*1000);
                    item.url = fields.url;
                    
                    //item = result
                    
                    output.results.push(item);
                });
                
                output.from = ((output.query.page-1) * output.query.items) || 0;
                output.to = output.from + (output.results && output.results.length || 0);
                
                if(output.to){
                    output.from++;
                }
                
                callback(null, output);
                
            });
          
        });
    });
}

function checkAllowedKeys(options, callback){
    var allowedKeys = "queryString, allResults, sort, page, items, language".split(/,\s*/),
        return_options = {};
    
    options = options || {};

    
    allowedKeys.forEach(function(key){
        if(options[key]){
            return_options[key] = options[key];
        }
    });
    
    options.authData = {};
    
    callback(null, return_options);
}

/*

Query
- queryString
Sort
- date des:asc
- relevance
Page (1)
Items (10; 100)
 
*/

function buildQuery(options, callback){
    var query = {}, query_strings = [], querystr = [], must = [], dateRange, ranges = {};
    if(typeof options == "string"){
        options = {
            queryString: options.trim()
        }
    }else{
        options = options || {};
    }
    
    if(options.queryString){
        options.queryString = (options.queryString || "").toString("utf-8").trim();
        querystr.push(options.queryString);
    }
    
    if(options.allResults){
        options.allResults = ["true", "1", "yes"].indexOf((options.allResults || "").toString("utf-8").toLowerCase().trim())>=0;
    }
    
    if(!options.queryString && !options.allResults){
        return callback(new Error("Empty query"));
    }
    
    options.page = Math.abs(parseInt(options.page, 10) || 1);
    options.items = Math.min(Math.abs(parseInt(options.items, 10) || 10), 100);
    
    if(options.page>1){
        query.from = (options.page - 1) * options.items;
    }
    query.size = options.items;
    
    query.highlight = {
        fields : {
            title : {"pre_tags" : ["<strong>"], "post_tags" : ["</strong>"]},
            contents : {"fragment_size" : 150, "number_of_fragments" : 3, "pre_tags" : ["<strong>"], "post_tags" : ["</strong>"]}
        }
    }
    
    query.fields = ["title", "contents", "time", "domain", "url", "html"];
    
    switch(options.sort){
        case "relevance":
            break;
        case "date:asc":
            query.sort = {
                time: {
                    order: "asc"
                }
            }
            break;
        case "date:desc":
        case "date":
        default:
            query.sort = {
                time: {
                    order: "desc"
                }
            }
            break;
    }
    
    if(options.queryString){
        query_strings.push({
            query_string: {
                fields: ["title^5", "contents"],
                query: options.queryString,
                boost: 4,
                default_operator:"AND"
            }
        });
    }
    
    if(options.allResults){
        query_strings.push({
            match_all: {}
        });
    }
    
    if(options.language){
        must.push({
            term: {
                language: options.language.toString()
            }
        });
    }

    function callbackStep1(){
        query.query = {};
        
        if(query_strings.length < 1){
            return callback(new Error("Empty query"));
        }else if(query_strings.length == 1 && !must.length){
            query.query = query_strings[0]; 
        }else{
            query.query.bool = {
                should: query_strings,
                minimum_number_should_match: 1
            }
            if(must){
                query.query.bool.must = must;
            }
        }

        if(Object.keys(ranges).length){
            query.query = {
                filtered : {
                    query: query.query,
                    filter: {
                        numeric_range: ranges
                    }
                }
            }
        }
    
        callback(null, options, query, querystr.join(" "));
    }

    if(options.queryString){
        lemmatizeQuery(options.queryString, function(err, lemmatizedQuery){
            if(err){
                lemmatizedQuery = "";
            }
            lemmatizedQuery = (lemmatizedQuery || "").toString("utf-8").trim();
            
            if(lemmatizedQuery.length){
                query_strings.push({
                    query_string: {
                        fields: ["lemma"],
                        query: lemmatizedQuery,
                        boost: 1,
                        default_operator:"AND"
                    }
                });
                querystr.push(lemmatizedQuery);
            }
            
            callbackStep1();
            
        });
    }else{
        callbackStep1();
    }
    
}

function makeQuery(query, callback){
    var url = "http://"+searchServerHost+":"+searchServerPort+"/articles/article/_search",
        options = {
            metod: "GET",
            payload: typeof query == "object" && JSON.stringify(query) || query.toString("utf-8")
        }
        
    fetch.fetchUrl(url, options, function(err, meta, body){
        var data;
        
        if(err){
            return callback(err);
        }
        if(!meta || !meta.status || meta.status >= 400){
            return callback(new Error("Invalid response from server ("+parseInt(meta.status,10)+")"));
        }
        
        try{
            data = JSON.parse(body.toString("utf-8"));
        }catch(E){
            return callback(E);
        }
        
        callback(null, data);
    });
}

function lemmatizeQuery(query, callback){
    var parsed = queryParser(query),
        data = "",
        job = gearman.submitJob("lemma", parsed.list.join(", "));

    job.setTimeout(1500, function(){
        job.abort();
        callback(new Error("Gearman job timeout exceeded"));
    });

    job.on("error", function(err){
        callback(err);
    });

    job.on("data", function(chunk){
        if(chunk){
            data += chunk.toString("binary");
        }
    });

    job.on("end", function(){
        var response, lemmas = {};
        
        data = new Buffer(data, "binary").toString("utf-8");
        
        response = data.trim().split(/\s*,\s*/);
        if(!response){
            return callback(new Error("Invalid response from Gearman worker"));
        }    
        
        for(var i=0, len = parsed.list.length; i<len; i++){
            lemmas[parsed.list[i]] = response[i] || parsed.list[i];
        }
        
        parsed.map.forEach(function(word){
            var lemma = lemmas[word.word] || word.word || "";
            switch(word.txtcase){
                case "allcaps":
                    lemma = lemma.toUpperCase();
                    break;
                case "firstcaps":
                    lemma = lemma.toLowerCase();
                    lemma = (lemma.charAt(0) || "").toUpperCase() + lemma.slice(1);
                    break;
                default:
                    lemma = lemma.toLowerCase();
            }
            if(parsed.words[word.pos].toLowerCase() != lemma.toLowerCase()){
                parsed.words[word.pos] = lemma;
            }
        });
        
        callback(null, parsed.words.join(""));
    });
}


function getTotalDocs(callback){
    var url = "http://"+searchServerHost+":"+searchServerPort+"/articles/_status";
    fetch.fetchUrl(url, function(err, meta, body){
        var data, docs;
        
        if(err){
            return callback(err);
        }
        if(!meta || !meta.status || meta.status >= 400){
            return callback(new Error("Invalid response from server ("+parseInt(meta.status,10)+")"));
        }
        
        try{
            data = JSON.parse(body.toString("utf-8"));
            docs = parseInt(data.indices.articles.docs.num_docs, 10);
        }catch(E){
            return callback(E);
        }
        
        callback(null, docs);
    });
}
