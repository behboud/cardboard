var s2 = require('s2'),
    through = require('through2'),
    _ = require('lodash'),
    geojsonStream = require('geojson-stream'),
    concat = require('concat-stream'),
    geojsonCover = require('geojson-cover'),
    coverOpts = require('./lib/coveropts'),
    uniq = require('uniq'),
    geobuf = require('geobuf'),
    log = require('debug')('cardboard'),
    queue = require('queue-async'),
    Dyno = require('dyno'),
    AWS = require('aws-sdk');

var MAX_ENTRY_BYTES = 64 * 1000; // 64KB

var emptyFeatureCollection = {
    type: 'FeatureCollection',
    features: []
};

module.exports = Cardboard;

function Cardboard(c) {
    if (!(this instanceof Cardboard)) return new Cardboard(c);
    this.dyno = Dyno(c);

    AWS.config.update({
        accessKeyId: c.awsKey,
        secretAccessKey: c.awsSecret,
        region: c.region || 'us-east-1',
    });
    if(c.s3) console.log('fake s3');

    this.s3 = c.s3 || new AWS.S3();
    this.bucket = c.bucket;
    this.prefix = c.prefix;
    coverOpts = c.coverOpts || coverOpts;
}

Cardboard.prototype.insert = function(primary, feature, layer, cb) {
    var indexes = geojsonCover.geometryIndexes(feature.geometry, coverOpts);
    var dyno = this.dyno;
    var s3 = this.s3;

    var q = queue(1);
    var buf = geobuf.featureToGeobuf(feature).toBuffer();

    // decide to save val with the item based on buf size.
    console.log('insert', indexes.length, primary, buf.length);

    indexes.forEach(writeIndex);
    function writeIndex(index) {
        var id = 'cell!' + index + '!' + primary;
        q.defer(dyno.putItem, {
            id: id,
            layer: layer,
            geometryid: primary
        }, {errors:{throughput:10}});
    }

    q.defer(s3.putObject, {
        Key: [this.prefix, layer, primary].join('/'),
        Bucket: this.bucket,
        Body: buf
    })
    q.awaitAll(function(err, res) {
        cb(err);
    });
};

Cardboard.prototype.createTable = function(tableName, callback) {
    var table = require('./lib/table.json');
    table.TableName = tableName;
    this.dyno.createTable(table, callback);
};

Cardboard.prototype.del = function(primary, layer, callback) {
    var dyno = this.dyno;
    this.get(primary, layer, function(err, res) {
        if (err) return callback(err);
        var indexes = geojsonCover.geometryIndexes(res[0].val.geometry, coverOpts);
        var params = {
            RequestItems: {}
        };
        function deleteId(id) {
            return {
                DeleteRequest: {
                    Key: { id: { S: id }, layer: { S: layer } }
                }
            };
        }
        // TODO: how to get table name properly here.
        params.RequestItems.geo = [
            deleteId('id!' + primary + '!0')
        ];
        var parts = partsRequired(res[0].val);
        for (var i = 0; i < indexes.length; i++) {
            for (var j = 0; j < parts; j++) {
                params.RequestItems.geo.push(deleteId('cell!' + indexes[i] + '!' + primary + '!' + j));
            }
        }
        dyno.batchWriteItem(params, function(err, res) {
            callback(null);
        });
    });
};

function partsRequired(feature) {
    var buf = geobuf.featureToGeobuf(feature).toBuffer();
    return Math.ceil(buf.length / MAX_ENTRY_BYTES);
}

// Cardboard.prototype.get = function(primary, layer, callback) {
//     var dyno = this.dyno;
//     dyno.query({
//         id: { 'BEGINS_WITH': 'id!' + primary },
//         layer: { 'EQ': layer }
//     }, { pages: 0 }, function(err, res) {
//         console.error(err, res);
//         if (err) return callback(err);
//         callback(err, parseQueryResponse([res]));
//     });
// };

Cardboard.prototype.getFeatures = function(layer, features, callback) {
    var s3 = this.s3;
    var bucket = this.bucket;
    var prefix = this.prefix;
    var q = queue(100);
    features.forEach(fetch);
    function fetch(f){
        var key = [prefix,layer,f.geometryid].join('/');
        q.defer(s3.getObject, {
            Key: key,
            Bucket: bucket
        });
    }
    q.awaitAll(function(err, data){
        callback(err, data);
    });
}

Cardboard.prototype.bboxQuery = function(input, layer, callback) {
    var indexes = geojsonCover.bboxQueryIndexes(input, true, coverOpts);
    var q = queue(100);
    var dyno = this.dyno;
    var query = +new Date();
    log('querying with ' + indexes.length + ' indexes');
    indexes.forEach(function(idx) {
        q.defer(
            dyno.query,
            {
                id: { 'BETWEEN': [ 'cell!' + idx[0], 'cell!' + idx[1] ] },
                layer: { 'EQ': layer }
            },
            { pages: 0 }
        );
    });
    q.awaitAll(function(err, res) {
        if (err) return callback(err);
        query = (+new Date()) - query;

        var res = parseQueryResponse(res);

        var startfetch = + new Date();
        this.getFeatures(layer, res, featuresResp);
        function featuresResp(err, data){
            var gotfeatures = +new Date();
            res = data.map(function(i) {
                i.val = geobuf.geobufToFeature(i.Body);
                return i;
            });

            var ret = {
                data: res,
                bench:{
                    indexes:indexes.length,
                    parse: (+ new Date()) - gotfeatures,
                    fetch: gotfeatures - startfetch,
                    query: query
                }
            };
            callback(err, ret);
        }
    }.bind(this));
};

function parseQueryResponse(res) {

    res = res.map(function(r) {
        return r.items;
    });

    var flat = _(res).chain().flatten().sortBy(function(a) {
        return a.id;
    }).value();

    flat = uniq(flat, function(a, b) {
        return a.geometryid !== b.geometryid
    }, true);

    flat = _.values(flat);

    return flat;
}

Cardboard.prototype.dump = function(cb) {
    return this.dyno.scan(cb);
};

Cardboard.prototype.dumpGeoJSON = function(callback) {
    return this.dyno.scan(function(err, res) {
        if (err) return callback(err);
        return callback(null, {
            type: 'FeatureCollection',
            features: res.items.map(function(f) {
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
             this.push(geobuf.geobufToFeature(data.val));
             cb();
        }))
        .pipe(geojsonStream.stringify());
};

Cardboard.prototype.geojsonCover = geojsonCover;
