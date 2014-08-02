var assert = require('assert');

var coveropts = {};

// the maximum number of S2 cells used for any query coverage.
// - More = more complex queries
// - Fewer = less accurate queries
coveropts.max_query_cells = 100;

// The largest size of a cell permissable in a query.
coveropts.query_min_level = 1;

// The smallest size of a cell permissable in a query.
// - This must be >= INDEX_MAX_LEVEL
coveropts.query_max_level = 5;

// the maximum number of S2 cells used for any index coverage.
// - More = more accurate indexes
// - Fewer = more compact queries
coveropts.max_index_cells = 100;

// The largest size of a cell permissable in an index.
// - This must be <= QUERY_MIN_LEVEL
coveropts.index_min_level = 5;

// The smallest size of a cell permissable in an index.
coveropts.index_max_level = 8;

// The index level for point features only.
coveropts.index_point_level = 20;

assert.ok(coveropts.query_max_level >= coveropts.index_min_level,
    'query level and index level must correspond');

module.exports = coveropts;
