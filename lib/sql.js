/**
 * Module Dependencies
 */

var Firebird = require('node-firebird');
var _ = require('lodash');
var utils = require('./utils');

var sql = module.exports = {

    // Convert Firebird format to standard javascript object
    normalizeSchema: function (schema) {
        return _.reduce(schema, function (memo, field) {

            // Marshal Firebird fields' info to waterline collection semantics
            var attrName = field.name;

            switch (field.type) {
                case 7:
                    switch (field.subType) {
                        case 0: // SMALLINT
                            memo[attrName] = {
                                type: 'integer',
                                size: 16
                            };
                            break;
                        case 1: //NUMERIC(n,m)
                            memo[attrName] = {
                                type: 'float'
                            };
                            break;
                        case 3: //DECIMAL
                            memo[attrName] = {
                                type: 'float'
                            };
                            break;
                    }
                    break;
                case 8:
                    switch (field.subType) {
                        case 0: // INTEGER
                            memo[attrName] = {
                                type: 'integer'
                            };
                            break;
                        case 1: //NUMERIC(n,m)
                            memo[attrName] = {
                                type: 'float'
                            };
                            break;
                        case 3: //DECIMAL
                            memo[attrName] = {
                                type: 'float'
                            };
                            break;
                    }
                    break;
                case 9: //QUAD ???
                    memo[attrName] = {
                        type: 'integer'
                    };
                    break;
                case 10: //FLOAT
                    memo[attrName] = {
                        type: 'float'
                    };
                    break;
                case 12: //DATE
                    memo[attrName] = {
                        type: 'date'
                    };
                    break;
                case 13: //TIME
                    memo[attrName] = {
                        type: 'time'
                    };
                    break;
                case 14: //CHAR
                    memo[attrName] = {
                        type: 'string',
                        size: field.length / field.bytesPerCharacter
                    };
                    break;
                case 16:
                    switch (field.subType) {
                        case 0: // BIGINT
                            memo[attrName] = {
                                type: 'integer',
                                size: 64
                            };
                            break;
                        case 1: //NUMERIC(n,m)
                            memo[attrName] = {
                                type: 'float'
                            };
                            break;
                        case 3: //DECIMAL
                            memo[attrName] = {
                                type: 'float'
                            };
                            break;
                    }
                    break;
                case 27: //DOUBLE
                    memo[attrName] = {
                        type: 'float'
                    };
                    break;
                case 35: //TIMESTAMP
                    memo[attrName] = {
                        type: 'timestamp'
                    };
                    break;
                case 37: //VARCHAR
                    memo[attrName] = {
                        type: 'string',
                        size: field.length / field.bytesPerCharacter
                    };
                    break;
                case 40: //CSTRING
                    memo[attrName] = {
                        type: 'string',
                        size: field.length / field.bytesPerCharacter
                    };
                    break;
                case 45: //BLOB_ID ???
                    memo[attrName] = {
                        type: 'text'
                    };
                    break;
                case 261:
                    switch (field.subType) {
                        case 0: // BLOB SUB_TYPE 0
                            memo[attrName] = {
                                type: 'binary'
                            };
                            break;
                        case 1: // BLOB SUB_TYPE 1
                            memo[attrName] = {
                                type: 'text' //or json or array
                            };
                            break;
                    }
                    break;
            }

            memo[attrName] = memo[attrName] || {};

            if (field.source) {
                memo[attrName].defaultsTo = field.source;
            }

            if (field.notNull) {
                memo[attrName].required = true;
            }

            if (field.primaryKey) {
                memo[attrName].primaryKey = field.primaryKey;
            }

            if (field.unique) {
                memo[attrName].unique = field.unique;
            }

            if (field.indexed) {
                memo[attrName].indexed = field.indexed;
            }

            return memo;
        }, {});
    },

    // @returns ALTER query for adding a column
    addColumn: function (collectionName, attrName, attrDef) {
        // Escape table name and attribute name
        var tableName = utils.escapeId(collectionName);

        // sails.log.verbose("ADDING ",attrName, "with",attrDef);

        // Build column definition
        var columnDefinition = sql._schema(collectionName, attrDef, attrName);

        return 'ALTER TABLE ' + tableName + ' ADD ' + columnDefinition;
    },

    // @returns ALTER query for dropping a column
    removeColumn: function (collectionName, attrName) {
        // Escape table name and attribute name
        var tableName = utils.escapeId(collectionName);
        attrName = utils.escapeId(attrName);

        return 'ALTER TABLE ' + tableName + ' DROP COLUMN ' + attrName;
    },

    // Create a schema csv for a DDL query
    schema: function (collectionName, attributes) {
        return sql.build(collectionName, attributes, sql._schema);
    },

    _schema: function (collectionName, attribute, attrName) {
        attrName = utils.escapeId(attrName);
        var type = sqlTypeCast(attribute);

        // Process PK field
        if (attribute.primaryKey) {

            // If type is an integer, set auto increment
            // TODO: Create primary key index
            if (type === 'SMALLINT' || type === 'INTEGER' || type === 'BIGINT') {
                // TODO: Create autoinc generator
                return attrName + ' ' + type + ' NOT NULL';
            }

            // Just set NOT NULL on other types
            return attrName + ' VARCHAR(255) NOT NULL';
        }

        // Process NOT NULL field.
        // if notNull is true, set NOT NULL constraint
        var nullPart = '';
        if (attribute.notNull) {
            nullPart = ' NOT NULL ';
        }

        // Process UNIQUE field
        if (attribute.unique) {
            // TODO: create unique index
            return attrName + ' ' + type + nullPart + ' UNIQUE KEY';
        }

        // Process INDEX field (NON-UNIQUE KEY)
        if (attribute.index) {
            // TODO: create index
            return attrName + ' ' + type + nullPart + ', INDEX(' + attrName + ')';
        }

        return attrName + ' ' + type + ' ' + nullPart;
    },

    // Put together the CSV aggregation
    // separator => optional, defaults to ', '
    // keyOverride => optional, overrides the keys in the dictionary
    //          (used for generating value lists in IN queries)
    // parentKey => key of the parent to this object
    build: function (collectionName, collection, fn, separator, keyOverride, parentKey) {
        separator = separator || ', ';
        var $sql = '';
        _.each(collection, function (value, key) {
            $sql += fn(collectionName, value, keyOverride || key, parentKey);

            // (always append separator)
            $sql += separator;
        });

        // (then remove final one)
        return String($sql).replace(new RegExp(separator + '+$'), '');
    }
};

// Cast waterline types into SQL data types
function sqlTypeCast(attr) {
    var type;
    if (_.isObject(attr) && _.has(attr, 'type')) {
        type = attr.type;
    } else {
        type = attr;
    }

    type = type && type.toLowerCase();

    switch (type) {
        case 'string':
        {
            var size = 255; // By default.

            // If attr.size is positive integer, use it as size of varchar.
            if (!Number.isNaN(attr.size) && (parseInt(attr.size) == parseFloat(attr.size)) && (parseInt(attr.size) > 0))
                size = attr.size;

            return 'VARCHAR(' + size + ')';
        }

        case 'text':
        case 'array':
        case 'json':
        {
            var size = 16384;
            if (!Number.isNaN(attr.segmentSize) && (parseInt(attr.segmentSize) == parseFloat(attr.segmentSize)) && (parseInt(attr.segmentSize) > 0))
                size = attr.segmentSize;
            return 'BLOB SUB_TYPE 1 SEGMENT SIZE ' + size;
        }

        case 'boolean':
            return 'SMALLINT';

        case 'integer':
        {
            var size = 32; // By default

            if (!Number.isNaN(attr.size) && (parseInt(attr.size) == parseFloat(attr.size)) && (parseInt(size) > 0)) {
                size = parseInt(attr.size);
            }

            switch (size) {
                case 8:
                    return 'SMALLINT';
                case 16:
                    return 'SMALLINT';
                case 32:
                    return 'INTEGER';
                case 64:
                    return 'INT64'
                default:
                    return 'INTEGER';
            }
        }

        case 'float':
            return 'DOUBLE PRECISION';

        case 'date':
            return 'DATE';

        case 'time':
            return 'TIME';

        case 'datetime':
            return 'TIMESTAMP';

        case 'binary':
        {
            var size = 16384;
            if (!Number.isNaN(attr.segmentSize) && (parseInt(attr.segmentSize) == parseFloat(attr.segmentSize)) && (parseInt(attr.segmentSize) > 0))
                size = attr.segmentSize;
            return 'BLOB SUB_TYPE 0 SEGMENT SIZE ' + size;
        }

        default:
            console.error('Unregistered type given: ' + type);
            return 'VARCHAR(255)';
    }
}

fieldTypeToName = function(type, sub_type) {
    switch (type) {
        case 7:
        {
            var size = 255; // By default.

            // If attr.size is positive integer, use it as size of varchar.
            if (!Number.isNaN(attr.size) && (parseInt(attr.size) == parseFloat(attr.size)) && (parseInt(attr.size) > 0))
                size = attr.size;

            return 'VARCHAR(' + size + ')';
        }

        case 'text':
        case 'array':
        case 'json':
        {
            var size = 16384;
            if (!Number.isNaN(attr.segmentSize) && (parseInt(attr.segmentSize) == parseFloat(attr.segmentSize)) && (parseInt(attr.segmentSize) > 0))
                size = attr.segmentSize;
            return 'BLOB SUB_TYPE 1 SEGMENT SIZE ' + size;
        }

        case 'boolean':
            return 'SMALLINT';

        case 'integer':
        {
            var size = 32; // By default

            if (!Number.isNaN(attr.size) && (parseInt(attr.size) == parseFloat(attr.size)) && (parseInt(size) > 0)) {
                size = parseInt(attr.size);
            }

            switch (size) {
                case 8:
                    return 'SMALLINT';
                case 16:
                    return 'SMALLINT';
                case 32:
                    return 'INTEGER';
                case 64:
                    return 'INT64'
                default:
                    return 'INTEGER';
            }
        }

        case 'float':
            return 'DOUBLE PRECISION';

        case 'date':
            return 'DATE';

        case 'time':
            return 'TIME';

        case 'datetime':
            return 'TIMESTAMP';

        case 'binary':
        {
            var size = 16384;
            if (!Number.isNaN(attr.segmentSize) && (parseInt(attr.segmentSize) == parseFloat(attr.segmentSize)) && (parseInt(attr.segmentSize) > 0))
                size = attr.segmentSize;
            return 'BLOB SUB_TYPE 0 SEGMENT SIZE ' + size;
        }

        default:
            console.error('Unregistered type given: ' + type);
            return 'VARCHAR(255)';
    }
};