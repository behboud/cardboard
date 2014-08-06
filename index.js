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
    AWS = require('aws-sdk');

var coverOpts = {};
coverOpts.max_query_cells = 100;
coverOpts.query_min_level = 5;
coverOpts.query_max_level = 5;
coverOpts.max_index_cells = 100;
coverOpts.index_min_level = 5;
coverOpts.index_max_level = 5;
coverOpts.index_point_level = 5;

module.exports = Cardboard;

function Cardboard(c) {
    if (!(this instanceof Cardboard)) return new Cardboard(c);

    AWS.config.update({
        accessKeyId: c.awsKey,
        secretAccessKey: c.awsSecret,
        region: c.region || 'us-east-1',
    });
    if(c.coverOpts) {
        coverOpts = c.coverOpts
    }
    this.bucket = c.bucket || 'mapbox-s2';
    this.prefix = c.prefix || 'dev';
    this.s3 = new AWS.S3();
}
var cells = {};

Cardboard.prototype.insert = function(primary, feature, layer) {
    var indexes = geojsonCover.geometryIndexes(feature.geometry, coverOpts);
    if(!feature.properties) feature.properties = {};
    feature.properties.id = primary;
    log('indexing ' + primary + ' with ' + indexes.length + ' indexes');

    indexes.forEach(updateCell);
    function updateCell(index) {
        if(cells[index]) {
            cells[index].features.push(feature);
        } else {
            cells[index] = {type:'FeatureCollection', features:[feature]};
        }
    }
};

Cardboard.prototype.finishInsert = function(layer, callback) {
    var q = queue(10);
    var s3 = this.s3;
    var bucket = this.bucket;
    var prefix = this.prefix;

    _(cells).each(function(val, key){
        var key = [prefix, layer, 'cell', key].join('/');
        q.defer(s3.putObject.bind(s3),{
            Key: key,
            Bucket: bucket,
            Body: geobuf.featureCollectionToGeobuf(val).toBuffer()
        });
    });
    q.awaitAll(callback);
};


// Cardboard.prototype.createTable = function(tableName, callback) {
//     var table = require('./lib/table.json');
//     table.TableName = tableName;
//     this.dyno.createTable(table, callback);
// };

Cardboard.prototype.bboxQuery = function(input, layer, callback) {
    var indexes = geojsonCover.bboxQueryIndexes(input, false, coverOpts);
    var q = queue(100);
    var s3 = this.s3;
    var prefix = this.prefix;
    var bucket = this.bucket;
    log('querying with ' + indexes.length + ' indexes');
    var bench = {query: +new Date()};
    indexes.forEach(function(idx) {

         function getCell(k, cb) {
            s3.getObject({
                Key: k,
                Bucket: bucket
            }, function(err, data){
                if(err && err.code !== 'NoSuchKey') {
                    console.error(err);
                    throw err;
                }
                cb(null, data);
            });
        }
        var key = [prefix, layer, 'cell', idx].join('/');
        q.defer(getCell, key);
    });
    q.awaitAll(function(err, res) {
        bench.query = (+new Date()) - bench.query;
        if (err) return callback(err);
        bench.parse = +new Date();


        var features = [];

        res = res.forEach(function(r) {
            if (r && r.Body) {
                features = features.concat(geobuf.geobufToFeatureCollection(r.Body).features);
            }
        });

        var features = _(features).compact().sortBy(function(a) {
             return a.properties.id;
        }).value();

        features = uniq(features, function(a, b) {
            return a.properties.id !== b.properties.id;
        }, true);
        bench.parse = (+new Date()) - bench.parse;

        features= features.map(function(f){
            return {val:f};
        })
        callback(err, {bench:bench, data:features});
    });
};

// Cardboard.prototype.dump = function(cb) {
//     return this.dyno.scan(cb);
// };
//
// Cardboard.prototype.dumpGeoJSON = function(callback) {
//     return this.dyno.scan(function(err, res) {
//         if (err) return callback(err);
//         return callback(null, {
//             type: 'FeatureCollection',
//             features: res.items.map(function(f) {
//                 return {
//                     type: 'Feature',
//                     properties: {
//                         key: f.key
//                     },
//                     geometry: new s2.S2Cell(new s2.S2CellId()
//                         .fromToken(
//                             f.key.split('!')[1])).toGeoJSON()
//                 };
//             })
//         });
//     });
// };
//
// Cardboard.prototype.export = function(_) {
//     return this.dyno.scan()
//         .pipe(through({ objectMode: true }, function(data, enc, cb) {
//              this.push(geobuf.geobufToFeature(data.val));
//              cb();
//         }))
//         .pipe(geojsonStream.stringify());
// };

Cardboard.prototype.geojsonCover = geojsonCover;
