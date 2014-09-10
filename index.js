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
    // In the insert case this operation should not be retried. The caller is given
    // a primary key and advised to retry the insert via cardboard.insert
    cardboard.put = function(feature, dataset, callback) {
        var f = _.clone(feature);
        if (!f.id) {
            f.id = cuid();
            cardboard.insert(f, dataset, function(err, res) {
                if (err) return callback(err, f.id);
                callback(null, res);
            });
        } else {
            cardboard.update(f, dataset, callback);
        }
    };

    // Retryable insert operation. Does not update indexes.
    // - feature: GeoJSON object with a specified feature.id
    // - dataset: the name of the cardboard dataset
    // - callback: if an error is encountered and the second argument passed to
    //     callback is truthy, then the caller is advised to retry the request. 
    //     If the second argument is falsey then the caller is advised not to retry
    //     the request. Usually this means that the request will always fail.
    cardboard.insert = function(feature, dataset, callback) {
        if (!feature.id) return callback(new Error('Feature does not specify an id'));

        var metadata = Metadata(dyno, dataset),
            timestamp = (+new Date()),
            primary = feature.id,
            buf = geobuf.featureToGeobuf(feature).toBuffer(),
            bounds = extent(feature),
            useS3 = buf.length > MAX_GEOMETRY_SIZE,
            s3Key = [prefix, dataset, primary, timestamp].join('/'),
            s3Params = { Bucket: bucket, Key: s3Key, Body: buf };
        
        var item = {
            dataset: dataset,
            id: ['id', primary].join('!'),
            timestamp: timestamp
        };

        if (useS3) item.geometryid = s3Key;
        else item.val = buf;
            
        var condition = { expected: {} };
        condition.expected.id = { ComparisonOperator: 'NULL' };

        var q = queue(1);
        if (useS3) q.defer(s3.putObject.bind(s3), s3Params);
        q.defer(dyno.putItem, item, condition);
        q.await(function(err) {
            if (err && err.code !== 'ConditionalCheckFailedException') return callback(err, true);
            updateMetadata();
        });

        function updateMetadata() {
            queue(1)
                .defer(metadata.defaultInfo)
                .defer(metadata.adjustBounds, bounds)
                .await(function(err) {
                    if (err) return callback(err, true);
                    callback(null, { id: primary, timestamp: timestamp });
                });
        }
    };

    // Retryable update operation. Does not update indexes.
    // - feature: GeoJSON object with a specified feature.id
    // - dataset: the name of the cardboard dataset
    // - callback: if an error is encountered, and the second argument passed to
    //     callback is truthy, then the caller is advised to retry the request. 
    //     If the second argument is falsey then the caller is advised not to retry
    //     the request. Usually this means that the request will always fail.
    cardboard.update = function(feature, dataset, callback) {
        if (!feature.id) return callback(new Error('Feature does not specify an id'));

        var metadata = Metadata(dyno, dataset),
            timestamp = (+new Date()),
            primary = feature.id,
            s3Key = [prefix, dataset, primary, timestamp].join('/'),
            buf = geobuf.featureToGeobuf(feature).toBuffer(),
            bounds = extent(feature),
            useS3 = buf.length > MAX_GEOMETRY_SIZE,
            s3Params = { Bucket: bucket, Key: s3Key, Body: buf },
            key = { dataset: dataset, id: ['id', primary].join('!') },
            query = { dataset: { EQ: dataset }, id: { EQ: key.id } };

        var item = { put: {}, delete: {} };
        item.put.timestamp = timestamp;
        if (useS3) {
            item.put.geometryid = s3Key;
            item.delete.val = '';
        } else {
            item.delete.geometryid = '';
            item.put.val = buf;
        }
            
        dyno.getItem(key, function(err, original) {
            if (err) return callback(err, true);

            // advise not to retry, feature does not exist to be updated
            if (!original.Item) 
                return callback(new Error('Update failed. Feature does not exist'), false);

            var updatedItem;
            var condition = { expected: {} };
            condition.expected.timestamp = { 
                ComparisonOperator: 'EQ',
                AttributeValueList: [ { N: original.Item.timestamp.toString() } ]
            };

            var q = queue(1);
            if (useS3) q.defer(s3.putObject.bind(s3), s3Params);
            q.defer(dyno.updateItem, key, item, condition);
            q.defer(metadata.defaultInfo);
            q.defer(metadata.adjustBounds, bounds);
            q.awaitAll(function(err, res) {
                // advise not to retry if you're trying to update out-of-order
                if (err && err.code === 'ConditionalCheckFailedException') return callback(err, false);
                if (err) return callback(err, true);
                callback(null, { id: primary, timestamp: timestamp });
            });
        });
    };

    // Retryable remove operation. Does not update indexes.
    // - primary: id of a feature
    // - dataset: the name of the cardboard dataset
    // - callback: if an error is encountered, and the second argument passed to
    //     callback is truthy, then the caller is advised to retry the request. 
    //     If the second argument is falsey then the caller is advised not to retry
    //     the request. Usually this means that the request will always fail.
    cardboard.remove = function(primary, dataset, updateIndex, callback) {
        if (typeof updateIndex === 'function') {
            callback = updateIndex;
            updateIndex = false;
        }

        var key = { dataset: dataset, id: ['id', primary].join('!') };

        dyno.deleteItems([ key ], function(err) {
            if (err) return callback(err, true);
            else callback();
        });
    };

    cardboard.get = function(primary, dataset, callback) {
        dyno.getItem({
            dataset: dataset,
            id: ['id', primary].join('!')
        }, function(err, res) {
            if (err) return callback(err);
            if (!res.Item) return callback(null, featureCollection());
            resolveFeature(res.Item, function(err, feature) {
                if (err) return callback(err);
                callback(null, featureCollection([feature]));
            });
        });
    };

    cardboard.getBySecondaryId = function(id, dataset, callback) {
        dyno.query({
            id: { 'BEGINS_WITH': 'usid!' + id },
            dataset: { 'EQ': dataset }
        }, function(err, res) {
            if (err) return callback(err);
            res = parseQueryResponse([res]);
            resolveFeatures(res, function(err, features) {
                if (err) return callback(err);
                callback(null, featureCollection(features));
            });
        });
    };

    cardboard.addFeatureIndexes = function(primary, dataset, timestamp, callback) {
        var key = { dataset: dataset, id: ['id', primary].join('!') };

        dyno.getItem(key, function(err, item) {
            if (err) return callback(err); // advise not to retry?
            if (item.Item.timestamp !== timestamp) return callback(new Error('Update applied out-of-order'), false);
            resolveFeature(item.Item, function(err, feature) {
                if (err) return callback(err); // TODO: unpack error cases here, how to advise?
                buildIndexes(feature);
            });
        });

        function buildIndexes(feature) {
            var usid = feature.properties && feature.properties.id ? feature.properties.id : null,
                level = indexLevel(feature),
                indexes = geojsonCover.geometryIndexes(feature.geometry, coverOpts[level]);

            var indexItems = indexes.map(function(index) {
                return {
                    dataset: dataset,
                    id: ['cell', level, index, primary].join('!'),
                    primary: primary
                };
            });

            if (usid) indexItems.push({
                dataset: dataset,
                id: ['usid', usid, primary].join('!'),
                primary: primary
            });

            queue(1)
                .defer(cardboard.removeFeatureIndexes, primary, dataset)
                .defer(dyno.putItems, indexItems)
                .await(callback); // TODO: advise retry all cases??
        }
    };

    cardboard.removeFeatureIndexes = function(primary, dataset, callback) {
        var query = { dataset: { EQ: dataset }, primary: { EQ: primary } },
            options = { index: 'primary', attributes: ['id'], pages: 0 };

        dyno.query(query, options, function(err, res) {
            if (err) return callback(err);

            var keys = res.items.map(function(item) {
                return { dataset: dataset, id: item.id };
            });

            dyno.deleteItems(keys, callback);
        });
    }

    cardboard.createTable = function(tableName, callback) {
        var table = require('./lib/table.json');
        table.TableName = tableName;
        dyno.createTable(table, callback);
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
            resolveFeatures(res, function(err, data) {
                if (err) return callback(err);
                callback(err, featureCollection(data));
            });
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
            return a.primary;
        }).value();

        flat = uniq(flat, function(a, b) {
            return a.primary !== b.primary
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

    function resolveFeature(item, callback) {
        var val = item.val,
            geometryid = item.geometryid,
            primary = item.primary;

        // Geobuf is stored in dynamo
        if (val) return callback(null, geobuf.geobufToFeature(val));
        
        // Geobuf is stored on S3
        if (geometryid) {
            return s3.getObject({
                Bucket: bucket,
                Key: geometryid
            }, function(err, data) {
                if (err) return callback(err);
                callback(null, geobuf.geobufToFeature(data.Body));
            });
        }

        // This is an index record with reference to a geometry record
        if (primary) {
            return dyno.getItem({
                dataset: item.dataset,
                id: ['id', primary].join('!')
            }, function(err, res) {
                if (err) return callback(err);
                resolveFeature(res.Item, callback);
            });
        }
        
        callback(new Error('No defined geometry'));
    }

    function resolveFeatures(items, callback) {
        var q = queue(100);
        items.forEach(function(item) {
            q.defer(resolveFeature, item);
        });
        q.awaitAll(callback);
    }

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
