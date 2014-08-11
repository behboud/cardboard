var assert = require('assert');

var coverOpts = {};

// the maximum number of S2 cells used for any query coverage.
// - More = more complex queries
// - Fewer = less accurate queries
coverOpts.MAX_QUERY_CELLS = 100;

// The largest size of a cell permissable in a query.
coverOpts.QUERY_MIN_LEVEL = 1;

// The smallest size of a cell permissable in a query.
// - This must be >= INDEX_MAX_LEVEL
coverOpts.QUERY_MAX_LEVEL = 8;

// the maximum number of S2 cells used for any index coverage.
// - More = more accurate indexes
// - Fewer = more compact queries
coverOpts.MAX_INDEX_CELLS = 100;

// The largest size of a cell permissable in an index.
// - This must be <= QUERY_MIN_LEVEL
coverOpts.INDEX_MIN_LEVEL = 8;

// The smallest size of a cell permissable in an index.
coverOpts.INDEX_MAX_LEVEL = 12;

// The index level for point features only.
coverOpts.INDEX_POINT_LEVEL = 15;

assert.ok(coverOpts.QUERY_MAX_LEVEL >= coverOpts.INDEX_MIN_LEVEL,
    'query level and index level must correspond');

module.exports = coverOpts;
