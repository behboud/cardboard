## [Cardboard(config)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L39-L643)


Cardboard client generator

### Parameters

* **config** `object` a configuration object


### Examples

```js
var cardboard = require('cardboard')({
  table: 'my-cardboard-table',
  region: 'us-east-1',
  bucket: 'my-cardboard-bucket',
  prefix: 'my-cardboard-prefix'
});
```
```js
var cardboard = require('cardboard')({
  dyno: require('dyno')(dynoConfig),
  bucket: 'my-cardboard-bucket',
  prefix: 'my-cardboard-prefix'
});
```

Returns `cardboard` a cardboard client





## [cardboard](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L57-L57)


A client configured to interact with a backend cardboard database








### [bboxQuery(bbox, dataset, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L517-L640)

Find GeoJSON features that intersect a bounding box

#### Parameters

* **bbox** `Array<number>` the bounding box as `[west, south, east, north]`
* **dataset** `string` the name of the dataset
* **callback** `function` the callback function to handle the response


#### Examples

```js
var bbox = [-120, 30, -115, 32]; // west, south, east, north
carboard.bboxQuery(bbox, 'my-dataset', function(err, collection) {
  if (err) throw err;
  collection.type === 'FeatureCollection'; // true
});
```



### [calculateDatasetInfo(dataset, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L455-L457)

Calculate metadata about a dataset

#### Parameters

* **dataset** `string` the name of the dataset
* **callback** `function` the callback function to handle the response


#### Examples

```js
cardboard.calculateDatasetInfo('my-dataset', function(err, metadata) {
  if (err) throw err;
  console.log(Object.keys(metadatata));
  // [
  //   'dataset',
  //   'id',
  //   'west',
  //   'south',
  //   'east',
  //   'north',
  //   'count',
  //   'size',
  //   'updated'
  // ]
});
```



### [createTable(tableName, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L234-L243)

Create a DynamoDB table with Cardboard's schema

#### Parameters

* **tableName** `[string]` the name of the table to create, if not provided, defaults to the tablename defined in client configuration.
* **callback** `function` the callback function to handle the response


#### Examples

```js
// Create the cardboard table specified by the client config
cardboard.createTable(function(err) {
  if (err) throw err;
});
```
```js
// Create the another cardboard table
cardboard.createTable('new-cardboard-table', function(err) {
  if (err) throw err;
});
```



### [del(primary, dataset, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L164-L172)

Remove a single GeoJSON feature

#### Parameters

* **primary** `string` the id for a feature
* **dataset** `string` the name of the dataset that this feature belongs to
* **callback** `function` the callback function to handle the response


#### Examples

```js
// Create a point, then delete it
var feature = {
  id: 'my-custom-id',
  type: 'Feature',
  properties: {},
  geometry: {
    type: 'Point',
    coordinates: [0, 0]
  }
};

cardboard.put(feature, 'my-dataset', function(err, result) {
  if (err) throw err;

  cardboard.del(result.id, 'my-dataset', function(err, result) {
    if (err) throw err;
    !!result; // true: the feature was removed
  });
});
```
```js
// Attempt to delete a feature that does not exist
cardboard.del('non-existent-feature', 'my-dataset', function(err, result) {
  err.message === 'Feature does not exist'; // true
  !!result; // false: nothing was removed
});
```



### [delDataset(dataset, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L266-L278)

Remove an entire dataset

#### Parameters

* **dataset** `string` the name of the dataset
* **callback** `function` the callback function to handle the response





### [get(primary, dataset, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L206-L217)

Retreive a single GeoJSON feature

#### Parameters

* **primary** `string` the id for a feature
* **dataset** `string` the name of the dataset that this feature belongs to
* **callback** `function` the callback function to handle the response


#### Examples

```js
// Create a point, then retrieve it.
var feature = {
  type: 'Feature',
  properties: {},
  geometry: {
    type: 'Point',
    coordinates: [0, 0]
  }
};

cardboard.put(feature, 'my-dataset', function(err, result) {
  if (err) throw err;
  result.geometry.coordinates = [1, 1];

  cardboard.get(result.id, 'my-dataset', function(err, final) {
    if (err) throw err;
    final === result; // true: the feature was retrieved
  });
});
```
```js
// Attempt to retrieve a feature that does not exist
cardboard.get('non-existent-feature', 'my-dataset', function(err, result) {
  err.message === 'Feature non-existent-feature does not exist'; // true
  !!result; // false: nothing was retrieved
});
```



### [getDatasetInfo(dataset, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L430-L432)

Get cached metadata about a dataset

#### Parameters

* **dataset** `string` the name of the dataset
* **callback** `function` the callback function to handle the response


#### Examples

```js
cardboard.getDatasetInfo('my-dataset', function(err, metadata) {
  if (err) throw err;
  console.log(Object.keys(metadatata));
  // [
  //   'dataset',
  //   'id',
  //   'west',
  //   'south',
  //   'east',
  //   'north',
  //   'count',
  //   'size',
  //   'updated'
  // ]
});
```



### [list(dataset, pageOptions, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L324-L383)

List the GeoJSON features that belong to a particular dataset

#### Parameters

* **dataset** `string` the name of the dataset
* **pageOptions** `[object]` pagination options
* **callback** `[function]` the callback function to handle the response


#### Examples

```js
// List all the features in a dataset
cardboard.list('my-dataset', function(err, collection) {
  if (err) throw err;
  collection.type === 'FeatureCollection'; // true
});
```
```js
// Stream all the features in a dataset
cardboard.list('my-dataset')
  .on('data', function(feature) {
    console.log('Got feature: %j', feature);
  })
  .on('end', function() {
    console.log('All done!');
  });
```
```js
// List one page with a max of 10 features from a dataset
cardboard.list('my-dataset', { maxFeatures: 10 }, function(err, collection) {
  if (err) throw err;
  collection.type === 'FeatureCollection'; // true
  collection.features.length <= 10; // true
});
```
```js
// Paginate through all the features in a dataset
(function list(startAfter) {
  var options = { maxFeatures: 10 };
  if (startAfter) options.start = startFrom;
  cardabord.list('my-dataset', options, function(err, collection) {
    if (err) throw err;
    if (!collection.features.length) return console.log('All done!');

    var lastId = collection.features.slice(-1)[0].id;
    list(lastId);
  });
})();
```

Returns `object` a readable stream


### [listDatasets(callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L395-L407)

List datasets available in this database

#### Parameters

* **callback** `function` the callback function to handle the response


#### Examples

```js
cardboard.listDatasets(function(err, datasets) {
  if (err) throw err;
  Array.isArray(datasets); // true
  console.log(datasets[0]); // 'my-dataset'
});
```



### [put(feature, dataset, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L117-L130)

Insert or update a single GeoJSON feature

#### Parameters

* **feature** `object` a GeoJSON feature
* **dataset** `string` the name of the dataset that this feature belongs to
* **callback** `function` the callback function to handle the response


#### Examples

```js
// Create a point, allowing Cardboard to assign it an id.
var feature = {
  type: 'Feature',
  properties: {},
  geometry: {
    type: 'Point',
    coordinates: [0, 0]
  }
};

cardboard.put(feature, 'my-dataset', function(err, result) {
  if (err) throw err;
  !!result.id; // true: an id has been assigned
});
```
```js
// Create a point, using a custom id.
var feature = {
  id: 'my-custom-id',
  type: 'Feature',
  properties: {},
  geometry: {
    type: 'Point',
    coordinates: [0, 0]
  }
};

cardboard.put(feature, 'my-dataset', function(err, result) {
  if (err) throw err;
  result.id === feature.id; // true: the custom id was preserved
});
```
```js
// Create a point, then move it.
var feature = {
  type: 'Feature',
  properties: {},
  geometry: {
    type: 'Point',
    coordinates: [0, 0]
  }
};

cardboard.put(feature, 'my-dataset', function(err, result) {
  if (err) throw err;
  result.geometry.coordinates = [1, 1];

  cardboard.put(result, 'my-dataset', function(err, final) {
    if (err) throw err;
    final.geometry.coordinates[0] === 1; // true: the feature was moved
  });
});
```




## [cardboard.batch](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/lib/batch.js#L19-L19)


A module for batch requests








### [put(collection, dataset, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/lib/batch.js#L29-L69)

Insert or update a set of GeoJSON features

#### Parameters

* **collection** `object` a GeoJSON FeatureCollection containing features to insert and/or update
* **dataset** `string` the name of the dataset that these features belongs to
* **callback** `function` the callback function to handle the response





### [remove(ids, dataset, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/lib/batch.js#L79-L94)

Remove a set of features

#### Parameters

* **ids** `Array<string>` an array of feature ids to remove
* **dataset** `string` the name of the dataset that these features belong to
* **callback** `function` the callback function to handle the response






## [cardboard.metadata](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L463-L463)


A module for incremental metadata adjustments








### [addFeature(dataset, feature, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L473-L475)

Incrementally update a dataset's metadata with a new feature. This operation **will** create a metadata record if one does not exist.

#### Parameters

* **dataset** `string` the name of the dataset
* **feature** `object` a GeoJSON feature (or backend record) being added to the dataset
* **callback** `function` a function to handle the response





### [deleteFeature(dataset, feature, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L499-L501)

Given a GeoJSON feature to remove, perform all required metadata updates. This operation **will not** create a metadata record if one does not exist. This operation **will not** shrink metadata bounds.

#### Parameters

* **dataset** `string` the name of the dataset
* **feature** `object` a GeoJSON feature (or backend record) to remove from the dataset
* **callback** `function` a function to handle the response





### [updateFeature(dataset, from, to, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/index.js#L487-L489)

Update a dataset's metadata with a change to a single feature. This operation **will not** create a metadata record if one does not exist.

#### Parameters

* **dataset** `string` the name of the dataset
* **from** `object` a GeoJSON feature (or backend record) representing the state of the feature *before* the update
* **to** `object` a GeoJSON feature (or backend record) representing the state of the feature *after* the update
* **callback** `function` a function to handle the response






## [utils](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/lib/utils.js#L14-L14)


A module containing internal utility functions








### [idFromRecord(record)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/lib/utils.js#L100-L105)

Strips database-information from a DynamoDB record's id

#### Parameters

* **record** `object` a DynamoDB record



Returns `string` id - the feature's identifier


### [resolveFeatures(dynamoRecords, callback)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/lib/utils.js#L21-L45)

Convert a set of backend records into a GeoJSON features

#### Parameters

* **dynamoRecords** `Array<object>` an array of items returned from DynamoDB in simplified JSON format
* **callback** `function` a callback function to handle the response





### [toDatabaseRecord(feature, dataset)](https://github.com/mapbox/cardboard/blob/6b2a35f7d72af8768adf988ea1ac686aa55f7ad4/lib/utils.js#L63-L93)

Converts a single GeoJSON feature into backend format

#### Parameters

* **feature** `object` a GeoJSON feature
* **dataset** `string` the name of the dataset the feature belongs to



Returns  the first element is a DynamoDB record suitable for inserting via `dyno.putItem`, the second are parameters suitable for uploading via `s3.putObject`.



