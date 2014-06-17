var q = require('q'),
    zlib = require('zlib'),
    byline = require('byline'),
    fs = require('fs'),
    gunzip = q.denodeify(zlib.unzip.bind(zlib)),
    readDir = q.denodeify(fs.readdir.bind(fs)),
    async = require('async'),
    os = require('os'),
    zlib = require('zlib'),
    NUMCORES = os.cpus().length,
    DATADIR = './data/';

var cluster = require("cluster");

if (cluster.isMaster) {
    cluster.setupMaster({
      exec: "./readldjson.js"
    });
    for (var i = 0; i < NUMCORES; i++) {
        cluster.fork();
    }
    return;
} else if (cluster.isWorker) {
    console.log('I am worker #' + cluster.worker.id);
}

var filter = function(fn) {
    return function(data) {
        return data.filter(fn);
    }
};

var splitBy = function(files, totalWorkers, currentWorker) {
    var partSize = files.length / NUMCORES;
    var start = currentWorker * partSize - partSize,
        end = start + partSize;

    return files.slice(Math.ceil(start), Math.floor(end) + 1);
};

readDir(DATADIR).then(filter(function(file) {
    return file.indexOf('.gz');
})).then(function(files) {
    return splitBy(files, NUMCORES, cluster.worker.id);
}).then(function(files) {
    console.log('Worker #' + cluster.worker.id + ' processing ' + files.length + ' files', files);
    async.each(files, function(file, done) {
        var src = DATADIR + file;
        var inp1 = fs.createReadStream(src);
        var gunzip = zlib.createGunzip();

        var stream = byline(inp1.pipe(gunzip));

        stream.on('data', function(line) {
            try {
                var obj = JSON.parse(line.toString('utf8'));
            } catch(e) {
                console.log(line.toString('utf8'), 'not valid');
            }
        });
        stream.on('end', function() {
            done();
        });
    }, function(err) {
        console.log('DONE');
        cluster.worker.disconnect();
    })
});
