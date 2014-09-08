var s2 = require('s2');
var through = require('through2');
var _ = require('lodash');
var geojsonStream = require('geojson-stream');
var geojsonNormalize = require('geojson-normalize')
var concat = require('concat-stream');
var geojsonCover = require('geojson-cover');
var coverOpts = require('./lib/coveropts');
var Metadata = require('./lib/metadata');
var uniq = require('uniq');
var geobuf = require('geobuf');
var log = require('debug')('cardboard');
var queue = require('queue-async');
var Dyno = require('dyno');
var AWS = require('aws-sdk');
var extent = require('geojson-extent');
var distance = require('turf-distance');
var point = require('turf-point');
var cuid = require('cuid');

var MAX_GEOMETRY_SIZE = 1024*10;  //10KB
var LARGE_INDEX_DISTANCE = 50; //bbox more then 100 miles corner to corner.


module.exports = function Cardboard(c) {
    var cardboard = {};

    var dyno = Dyno(c);
    AWS.config.update({
        accessKeyId: c.awsKey,
        secretAccessKey: c.awsSecret,
        region: c.region || 'us-east-1',
    });

    // allow for passed in config object to override s3 object for mocking in tests
    var s3 = c.s3 || new AWS.S3();
    if(!c.bucket) throw new Error('No bucket set');
    var bucket = c.bucket;
    if(!c.prefix) throw new Error('No s3 prefix set');
    var prefix = c.prefix;

    // If feature.id isnt set, this works like an insert, and assigns an id
    // if feature.id is set, its an update, if the feature doesnt exist it will fail.
    cardboard.put = function(feature, dataset, callback) {
        if(!feature.id){
            insert(feature, dataset, callback);
        } else {
            update(feature, dataset, callback);
        }
    };

    function insert(feature, dataset, callback) {
        var level = indexLevel(feature);
        var indexes = geojsonCover.geometryIndexes(feature.geometry, coverOpts[level]);
        var primary = cuid();
        var metadata = Metadata(dyno, dataset);

        log('insert', primary, (feature.properties ? feature.properties.id : 'undefined'), dataset, 'level:', level, 'indexes:', indexes.length);
        var q = queue();
        var buf = geobuf.featureToGeobuf(feature).toBuffer();

        function item(id) {
            var obj = {
                id: id,
                dataset: dataset,
                geometryid: primary
            };

            if(buf.length <= MAX_GEOMETRY_SIZE){
                obj.val = buf;
            }
            return obj;
        }
        var items = [];
        for(var i=0; i < indexes.length; i++) {
            items.push(item('cell!' + level + '!' + indexes[i] + '!' + primary));
        }
        items.push(item('id!' + primary));

        // If the user specified an id in properties, index it.
        if(feature.properties && feature.properties.id) {
            items.push(item('usid!' + feature.properties.id + '!' + primary));
        }

        q.defer(dyno.putItems, items, {errors:{throughput:10}});

        q.defer(s3.putObject.bind(s3), {
            Key: [prefix, dataset, primary].join('/'),
            Bucket: bucket,
            Body: buf
        });
        q.defer(metadata.addFeature, feature);
        q.awaitAll(function(err, res) {
            callback(err, primary);
        });
    }

    function update(feature, dataset, callback) {
        var level = indexLevel(feature);
        var indexes = geojsonCover.geometryIndexes(feature.geometry, coverOpts[level]);
        var buf = geobuf.featureToGeobuf(feature).toBuffer();

        log('update', feature.id, dataset, 'level:', level, 'indexes:', indexes.length);

        // load existing feature.
        cardboard.get(feature.id, dataset, gotFeature);
        function gotFeature(err, existingFeature) {
            if(err) return callback(err, null);
            if(existingFeature.features.length === 0) return callback(new Error('Update failed. Feature does not exist'));
            var existingLevel = indexLevel(existingFeature);
            var existingIndexes = geojsonCover.geometryIndexes(existingFeature.features[0], coverOpts[existingLevel]);
            var existingBuf = geobuf.featureToGeobuf(existingFeature.features[0]).toBuffer();
            // If existingLevel and level dont match, the feature crossed into a diff
            // sized index. Delete them all and insert again.

            var toDelete = _.difference(existingIndexes, indexes);
            var toInsert = _.difference(indexes, existingIndexes);
            var toUpdate = _.intersection(existingIndexes, indexes);
            var q = queue(50);

            var deleteKeys = toDelete.map(function(id){
                return {
                    dataset: dataset,
                    id: 'cell!' + existingLevel + '!' + id + '!' + feature.id
                };
            });

            var insertItems = [];
            for(var i=0; i < toInsert.length; i++) {
                insertItems.push(item('cell!' + level + '!' + toInsert[i] + '!' + feature.id));
            }
            q.defer(dyno.putItems, insertItems, {errors:{throughput:10}});

            var updateDoc;
            if(buf.length <= MAX_GEOMETRY_SIZE){
                updateDoc = { put: {val: buf }};
            }else if(buf.length > MAX_GEOMETRY_SIZE && existingBuf <= MAX_GEOMETRY_SIZE) {
                updateDoc = { delete: {val: buf }};
            }
            if(updateDoc){
                for(var i=0; i < toUpdate.length; i++) {
                    q.defer(dyno.updateItem,
                        {
                            id: 'cell!' + level + '!' + toUpdate[i] + '!' + feature.id,
                            dataset: dataset
                        },
                        updateDoc,
                        { errors: { throughput: 10 } }
                    );
                }

                q.defer(dyno.updateItem,
                    {
                        id: 'id!' + feature.id,
                        dataset: dataset
                    },
                    updateDoc,
                    { errors: { throughput: 10 } }
                );

                // If the user specified an id in properties, index it.
                if(feature.properties && feature.properties.id) {
                    q.defer(dyno.updateItem,
                        {
                            id: 'usid!' + feature.properties.id + '!' + feature.id,
                            dataset: dataset
                        },
                        updateDoc,
                        { errors: { throughput: 10 } }
                    );
                } else if(existingFeature.features[0].properties && existingFeature.features[0].properties.id) {
                    // they used to have a properties id. It was removed. Remove the item from the index
                    deleteKeys.push({
                        dataset: dataset,
                        id: 'usid!' + existingFeature.features[0].properties.id + '!' + feature.id
                    })
                }
            }
            q.defer(dyno.deleteItems, deleteKeys, {errors:{throughput:10}});
            q.defer(s3.putObject.bind(s3), {
                Key: [prefix, dataset, feature.id].join('/'),
                Bucket: bucket,
                Body: buf
            })
            q.awaitAll(function(err, res) {
                callback(err, feature.id);
            });
        };

        function item(id, update) {
            var obj = {
                id: id,
                dataset: dataset,
                geometryid: feature.id
            };

            if(buf.length < MAX_GEOMETRY_SIZE){
                obj.val = buf;
            }
            return obj;
        }
    }

    cardboard.createTable = function(tableName, callback) {
        var table = require('./lib/table.json');
        table.TableName = tableName;
        dyno.createTable(table, callback);
    };

    cardboard.delFeature = function(primary, dataset, callback) {
        cardboard.get(primary, dataset, function(err, res) {
            if (err) return callback(err);
            var metadata = Metadata(dyno, dataset);
            var indexes = geojsonCover.geometryIndexes(res.features[0], coverOpts);
            var params = {
                RequestItems: {}
            };

            var keys = [{ id: 'id!' + primary, dataset: dataset }];

            if(res.features[0].properties && res.features[0].properties.id) {
                keys.push({ id: 'usid!' + res.features[0].properties.id + '!' + primary, dataset: dataset })
            }

            for (var i = 0; i < indexes.length; i++) {
                keys.push({id: 'cell!' + indexes[i] + '!' + primary, dataset: dataset});
            }
            
            queue()
                .defer(dyno.deleteItems, keys)
                .defer(metadata.deleteFeature, res.features[0])
                .awaitAll(callback);
        });
    };

    cardboard.delDataset = function(dataset, callback) {
        cardboard.listIds(dataset, function(err, res) {
            var keys = res.map(function(id){
                return {
                    dataset: dataset,
                    id: id
                };
            });

            dyno.deleteItems(keys, function(err, res) {
                callback(err);
            });
        });
    };
    cardboard.getBySecondaryId = function(id, dataset, callback) {
        dyno.query({
            id: { 'BEGINS_WITH': 'usid!' + id },
            dataset: { 'EQ': dataset }
        }, function(err, res) {
            if (err) return callback(err);
            var res = parseQueryResponse([res]);
            getFeatures(dataset, res, featuresResp);

            function featuresResp(err, data) {
                data = data.map(function(i) {
                    i.val = geobuf.geobufToFeature(i.val);
                    i.val.id = i.geometryid;
                    return i;
                });
                var fc = featureCollection();
                res.forEach(function(i){
                    fc.features.push(_(data).findWhere({geometryid: i.geometryid}).val);
                });
                callback(err, fc);
            }
        });

    };
    cardboard.get = function(primary, dataset, callback) {
        dyno.query({
            id: { 'EQ': 'id!' + primary },
            dataset: { 'EQ': dataset }
        }, function(err, res) {
            if (err) return callback(err);
            var res = parseQueryResponse([res]);

            if(res.length === 0) return callback(null, featureCollection());

            if(res[0].val) {
                respond(res[0]);
            } else {
                getFeatures(dataset, res, function(err, result) {
                    if(err) return callback(err);
                    respond(result[0]);
                });
            }

            function respond(feature) {
                feature.val = geobuf.geobufToFeature(feature.val);
                feature.val.id = feature.geometryid;
                return callback(err, featureCollection([feature.val]));
            }
        });
    };

    function getFeatures(dataset, features, callback) {
        var q = queue(100);
        features.forEach(function(f) {
            q.defer(fetch, f);
        });
        function fetch(f, cb) {
            var key = [prefix, dataset, f.geometryid].join('/');
            if(f.val) {
                return cb(null, { geometryid: f.geometryid, val: f.val });
            }
            s3.getObject({
                Key: key,
                Bucket: bucket
            }, function(err, data) {
                cb(err, { geometryid:f.geometryid, val:data.Body });
            });
        }
        q.awaitAll(function(err, data) {
            callback(err, data);
        });
    };

    cardboard.list = function(dataset, callback) {
        dyno.query({
            dataset: { 'EQ': dataset },
            id: { 'BEGINS_WITH': 'id!' }
        }, function(err, res) {
            if (err) return callback(err);
            callback(err, parseQueryResponseId([res]));
        });
    };

    cardboard.listIds = function(dataset, callback) {
        dyno.query({
            dataset: { 'EQ': dataset }
        }, {
            attributes: ['id']
        }, function(err, res) {
            if (err) return callback(err);
            callback(err, res.items.map(function(_) {
                return _.id;
            }));
        });
    };

    cardboard.listDatasets = function(callback) {
        dyno.scan({
            attributes: ['dataset'],
            pages:0
        }, function(err, res) {
            if (err) return callback(err);
            var datasets = _.uniq(res.items.map(function(item){
                return item.dataset;
            }));
            callback(err, datasets);
        });
    };

    cardboard.getDatasetInfo = function(dataset, callback) {
        Metadata(dyno, dataset).getInfo(callback);
    };

    cardboard.bboxQuery = function(input, dataset, callback) {
        var q = queue(100);

        function queryIndexLevel(level) {
            var indexes = geojsonCover.bboxQueryIndexes(input, true, coverOpts[level]);

            log('querying level:', level, ' with ', indexes.length, 'indexes');
            indexes.forEach(function(idx) {
                q.defer(
                    dyno.query, {
                        id: { 'BETWEEN': [ 'cell!'+level+'!' + idx[0], 'cell!'+level+'!' + idx[1] ] },
                        dataset: { 'EQ': dataset }
                    },
                    { pages: 0 }
                );
            });
        }

        [0,1].forEach(queryIndexLevel);

        q.awaitAll(function(err, res) {
            if (err) return callback(err);
            var res = parseQueryResponse(res);
            getFeatures(dataset, res, featuresResp);
            function featuresResp(err, data) {
                data = data.map(function(i) {
                    i.val = geobuf.geobufToFeature(i.val);
                    i.val.id = i.geometryid;
                    return i;
                });

                var fc = featureCollection();
                res.forEach(function(i){
                    fc.features.push(_(data).findWhere({geometryid: i.geometryid}).val);
                });
                callback(err, fc);
            }
        });
    };

    function parseQueryResponseId(res) {
        res = res.map(function(r) {
            return r.items.map(function(i) {
                i.id_parts = i.id.split('!');
                return i;
            });
        });

        var flat = _(res).chain().flatten().sortBy(function(a) {
            return a.id_parts[1];
        }).value();

        flat = uniq(flat, function(a, b) {
            return a.id_parts[1] !== b.id_parts[1] ||
                a.id_parts[2] !== b.id_parts[2];
        }, true);

        flat = _.groupBy(flat, function(_) {
            return _.id_parts[1];
        });

        flat = _.values(flat);

        flat = flat.map(function(_) {
            var concatted = Buffer.concat(_.map(function(i) {
                return i.val;
            }));
            _[0].val = concatted;
            return _[0];
        });

        return flat.map(function(i) {
            i.val = geobuf.geobufToFeature(i.val);
            return i;
        });
    }

    function parseQueryResponse(res) {

        res = res.map(function(r) {
            return r.items;
        });

        var flat = _(res).chain().flatten().sortBy(function(a) {
            return a.geometryid;
        }).value();

        flat = uniq(flat, function(a, b) {
            return a.geometryid !== b.geometryid
        }, true);

        flat = _.values(flat);

        return flat;
    }

    cardboard.dump = function(cb) {
        return dyno.scan(cb);
    };

    cardboard.dumpGeoJSON = function(callback) {
        return dyno.scan(function(err, res) {
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

    cardboard.export = function(_) {
        return dyno.scan()
            .pipe(through({ objectMode: true }, function(data, enc, cb) {
                 this.push(geobuf.geobufToFeature(data.val));
                 cb();
            }))
            .pipe(geojsonStream.stringify());
    };
    return cardboard;
};

function indexLevel(feature) {
    var bbox = extent(feature);
    var sw = point(bbox[0], bbox[1]);
    var ne = point(bbox[2], bbox[3]);
    var dist = distance(sw, ne, 'miles');
    return dist >= LARGE_INDEX_DISTANCE ? 0 : 1;
}

function featureCollection(features) {
    return {
        type: 'FeatureCollection',
        features: features || []
    };
}
