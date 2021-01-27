var mysql = require("mysql")
var fs = require("fs")
var events = require("events")
var eventEmitter = new events.EventEmitter()

var config = require("./"+process.argv[2])
var mongodb_uri = config.mongo.uri
var pool = mysql.createPool({
  host: config.mysql.host,
  user: config.mysql.user,
  password: config.mysql.password,
  database: config.mysql.database
})

var cnt_documents = 0;

var getConnection = function(callback){
  pool.getConnection(function(err, connection){
    callback(err,connection)
  })
}

if (!fs.existsSync("json")) {
  fs.mkdirSync("json");
}

getConnection(function(err,connection) {
    if (err) {
      throw err;
    }
    connection.query("SELECT TABLE_NAME  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='"+config.mysql.database+"';", function(err, names){
      if (err) {
          throw err;
      }
      cnt_documents = names.length
      for (const name of names) {
        let table_name = name["TABLE_NAME"]
        connection.query("SELECT column_name,column_type FROM information_schema.columns WHERE table_name='"+table_name+"' order by ORDINAL_POSITION",function(err, data){
          if (err ) {
            throw err;
          }
          export_mysql_table(table_name)
        })
      }
  }) 
  connection.release()
})

async function export_mysql_table(table_name) {

  var Transform = require("stream").Transform;
  var transform = (Transform({objectMode:true}));
  var writeFile = "json/" + table_name + ".json";
  var writeStream = fs.createWriteStream(writeFile)
  writeStream.write("[")

  transform._transform = function(data, encoding,callback) {
    writeStream.write(JSON.stringify(data)+",")
    callback()
  };

    console.log("exporting table ... ", table_name)
    
    getConnection(function(err,connection){
      if (err) {
        throw err;
      }

      connection.query('select * from ' + table_name)
          .stream()
          .pipe(transform)
          .on('finish',function() { 
              writeStream.write("{}]")
              connection.release()
              console.log("export complete : " , table_name);
              import_mongo(table_name)
      })
  })
}

async function import_mongo(file_name) {
  console.log("importing into mongodb ... ", file_name )
  let exec = require('child_process').exec;
  let exec_process = exec("mongoimport --uri "+mongodb_uri+" --collection "+file_name+" --file json/" +file_name+".json --jsonArray",
    function(error,stderr,stdout) {
      if (error !== null) {
        console.log(`exec error: ${error}`);
      }
      console.log(`stdout: ${stdout}`);
  });
  exec_process.on("close",function(){
    eventEmitter.emit("import_complete")
  })
}

eventEmitter.on("import_complete", function(event) {
  cnt_documents = cnt_documents - 1;
  if (cnt_documents == 0) {
    fs.rmdirSync("./json", { recursive: true });
    process.exit(0)
  }
})