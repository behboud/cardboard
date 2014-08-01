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
    extent = require('turf-extent'),
    distance = require('turf-distance'),
    point = require('turf-point'),
    AWS = require('aws-sdk');

var coverOpts = {}; 
coverOpts.max_query_cells = 100;
coverOpts.query_min_level = 5;
coverOpts.query_max_level = 5;
coverOpts.max_index_cells = 100;
coverOpts.index_min_level = 5;
coverOpts.index_max_level = 5;
coverOpts.index_point_level = 5;

var coverOptsBig = {};
coverOptsBig.max_query_cells = 100;
coverOptsBig.query_min_level = 4;
coverOptsBig.query_max_level = 4;
coverOptsBig.max_index_cells = 100;
coverOptsBig.index_min_level = 4;
coverOptsBig.index_max_level = 4;
coverOptsBig.index_point_level = 15;

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

Cardboard.prototype.insert = function(primary, feature, layer, cb) {
    isBig(feature)
    var indexes = geojsonCover.geometryIndexes(feature.geometry, coverOpts);
    var s3 = this.s3;
    var bucket = this.bucket;
    var prefix = this.prefix;
    if(!feature.properties) feature.properties = {};
    feature.properties.id = primary;
    log('indexing ' + primary + ' with ' + indexes.length + ' indexes');
    var q = queue(1);

    function updateCell(key, feature, cb) {
        s3.getObject({Key:key, Bucket: bucket}, getObjectResp);
        function getObjectResp(err, data) {
            if (err && err.code !== 'NoSuchKey') {
                console.log('Error Read', err);
                throw err;
            }
            var fc;
            if (data && data.Body) {
                fc = geobuf.geobufToFeatureCollection(data.Body);
                fc.features.push(feature);

            } else {
                fc = {type:'FeatureCollection', features:[feature]};
            }
            s3.putObject(
                {
                    Key:key,
                    Bucket:bucket,
                    Body: geobuf.featureCollectionToGeobuf(fc).toBuffer()
                },
                putObjectResp);

        }
        function putObjectResp(err, data) {
            if(err) console.log(err)
            cb(err, data)
        }
    }


    indexes.forEach(function(index) {
        var key =  [prefix, layer, 'cell', index].join('/');
        q.defer(updateCell, key, feature);
    });
    q.awaitAll(function(err, res) {
        cb(err);
    });
};

Cardboard.prototype.bboxQuery = function(input, layer, callback) {
    var indexes = geojsonCover.bboxQueryIndexes(input, false, coverOpts);
    var q = queue(100);
    var s3 = this.s3;
    var prefix = this.prefix;
    var bucket = this.bucket;
    log('querying with ' + indexes.length + ' indexes');
    console.time('query');
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
        console.timeEnd('query');
        if (err) return callback(err);
        console.time('parse');


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
        console.timeEnd('parse');

        features= features.map(function(f){
            return {val:f};
        })
        callback(err, features);
    });
};

Cardboard.prototype.geojsonCover = geojsonCover;

function isBig(feature) {
    var bbox = extent(feature);
    var sw = point(bbox[0], bbox[1]);
    var ne = point(bbox[2], bbox[3]);
    var dist = distance(sw, ne, 'miles');
    return dist >= 100;
}