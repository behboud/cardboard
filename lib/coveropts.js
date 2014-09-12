var assert = require('assert');


// index at 2 levels, for large and small geometries.
var coveropts = {};


coveropts.max_index_cells = 1;

// The largest size of a cell permissable in an index.
// - This must be <= QUERY_MIN_LEVEL
coveropts.index_min_level = 0;

// The smallest size of a cell permissable in an index.
coveropts.index_max_level = 20;

// The index level for point features only.
coveropts.index_point_level = 20;


coveropts.query_min_level = 1;
coveropts.query_max_level = 20;
coveropts.max_index_cells = 1;


module.exports = coveropts;
