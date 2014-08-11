var s2 = require('s2'),
    through = require('through2'),
    _ = require('lodash'),
    geojsonStream = require('geojson-stream'),
    concat = require('concat-stream'),
    geojsonCover = require('geojson-cover'),
    uniq = require('uniq'),
    geobuf = require('geobuf'),
    log = require('debug')('cardboard'),
    queue = require('queue-async'),
    level = require('level'),
    coverOpts = require('./lib/coveropts');

var db;

module.exports = Cardboard;

function Cardboard(c) {
    if (!(this instanceof Cardboard)) return new Cardboard(c);
    db = level(c.db || './db', {valueEncoding: 'binary'});
    this.coverOpts = c.coverOpts || coverOpts;
}

Cardboard.prototype.insert = function(primary, feature, layer, callback) {
    var indexes = geojsonCover.geometryIndexes(feature.geometry, this.coverOpts);
    var buf = geobuf.featureToGeobuf(feature).toBuffer();
    var puts = [];

    indexes.forEach(writeIndex);
    function writeIndex(index) {
        puts.push({
            type: 'put',
            key: ['cell', index, primary].join('!'),
            value: 0
        });
    }
    puts.push({
        type: 'put',
        key: ['id', primary].join('!'),
        value: buf
    });

    var q  = queue();
    q.defer(db.batch.bind(db), puts);
    // also write to s3
    q.await(callback);
};

// Cardboard.prototype.del = function(primary, layer, callback) {
//     this.get(primary, layer, function(err, res) {
//         if (err) return callback(err);
//         var indexes = geojsonCover.geometryIndexes(res[0].value.geometry);
//         var params = {
//             RequestItems: {}
//         };
//         function deleteId(id) {
//             return {
//                 DeleteRequest: {
//                     Key: { id: { S: id }, layer: { S: layer } }
//                 }
//             };
//         }
//         // TODO: how to get table name properly here.
//         params.RequestItems.geo = [
//             deleteId('id!' + primary + '!0')
//         ];
//         var parts = partsRequired(res[0].value);
//         for (var i = 0; i < indexes.length; i++) {
//             for (var j = 0; j < parts; j++) {
//                 params.RequestItems.geo.push(deleteId('cell!' + indexes[i] + '!' + primary + '!' + j));
//             }
//         }
//         dyno.batchWriteItem(params, function(err, res) {
//             callback(null);
//         });
//     });
// };

Cardboard.prototype.get = function(primary, layer, callback) {
    db.get('id!' + primary, function(err, value) {
        if (err) return callback(err);
        callback(err, [{id:primary, val:geobuf.geobufToFeature(value)}]);
    });
};

function query(opts, callback){
    var results = [];
    db.createReadStream(opts)
    .on('data', function (data) {
        var parts = data.key.split('!');
        results.push({
            index: parts[1],
            primary: parts[2],
            val: data.value
        });
    })
    .on('error', function (err) {
        callback(err);
    })
    .on('end', function () {
        callback(null, results);
    });
}


Cardboard.prototype.bboxQuery = function(input, layer, callback) {
    var indexes = geojsonCover.bboxQueryIndexes(input, true, this.coverOpts);
    var q = queue(100);
    log('querying with ' + indexes.length + ' indexes');
    var bench = {query: +new Date()};

    indexes.forEach(function(idx) {
        q.defer(
            query,
            {
                gte:'cell!' + idx[0],
                lte:'cell!' + idx[1],
            }
        );
    });
    q.awaitAll(function(err, res) {
        bench.query = (+new Date()) - bench.query;
        bench.features = +new Date();
        if (err) return callback(err);
        res = parseQueryResponse(res);
        // get geometries
        var ids = res.map(function(r){
            return r.primary;
        });
        this.getBatch(ids, layer, results);
    }.bind(this));
    function results(err, data) {
        bench.features = (+new Date()) - bench.features;
        callback(err, {data: data, bench:bench});
    }
};

Cardboard.prototype.getBatch = function(ids, layer, callback) {
    var q = queue(100);

    ids.forEach(get);
    function get(primary){
        q.defer(function(cb){
            db.get('id!' + primary, function(err, value) {
                if (err) return cb(err);
                cb(err, {id:primary, val:geobuf.geobufToFeature(value)});
            });
        });
    }
    q.awaitAll(function(err, data){

        //console.log('data', data)
        callback(err, data);
    });
};

function parseQueryResponse(res) {

    var flat = _(res).chain().flatten().sortBy(function(a) {
        return a.primary;
    }).value();

    flat = uniq(flat, function(a, b) {
        return a.primary !== b.primary;
    }, true);

    return flat;
}

Cardboard.prototype.list = function(layer, callback) {
    query({gte:'id',lte:'id!!'}, function(err, res){
        if (err) return callback(err);
        var res = res.map(function(r){
            return {id: r.index, val: geobuf.geobufToFeature(r.val), layer: layer};
        });
        callback(err, res);
    });
};


Cardboard.prototype.dump = function(cb) {
    var data = [];
    db.createReadStream()
        .on('data', function(d){
            data.push(d);
        })
        .on('error', function(){
            cb(err);
        })
        .on('end', function(){
            cb(null, data);
        });
};

Cardboard.prototype.dumpGeoJSON = function(callback) {
    return this.dump(function(err, res) {
        if (err) return callback(err);
        return callback(null, {
            type: 'FeatureCollection',
            features: res.map(function(f) {
                return {
                    type: 'Feature',
                    properties: {
                        key: f.key
                    },
                    geometry: new s2.S2Cell(new s2.S2CellId()
                        .fromToken(
                            f.key.split('!')[1])).toGeoJSON()
                };
            })
        });
    });
};

Cardboard.prototype.export = function(_) {
    return this.dyno.scan()
        .pipe(through({ objectMode: true }, function(data, enc, cb) {
             this.push(geobuf.geobufToFeature(data.value));
             cb();
        }))
        .pipe(geojsonStream.stringify());
};

Cardboard.prototype.geojsonCover = geojsonCover;
