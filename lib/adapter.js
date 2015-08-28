// Dependencies
var async = require('async');
var _ = require('lodash');
var util = require('util');
var Firebird = require('node-firebird');

var Errors = require('waterline-errors').adapter;
var Sequel = require('waterline-sequel');
var Cursor = require('waterline-cursor');

var utils = require('./utils');
var _teardownConnection = require('./connections/teardown');
var _spawnConnection = require('./connections/spawn');
var _registerConnection = require('./connections/register');

var sql = require('./sql.js');

var hop = utils.object.hasOwnProperty;

var STRINGFILE = {
    noCallbackError: 'An error occurred in the Firebird adapter, but no callback was specified to the spawnConnection function to handle it.'
};

// Hack for development - in future versions, allow
// logger to be injected (see wl2
// or tweet @mikermcneil for status of this feature or
// to help out)
var log = (process.env.LOG_QUERIES === 'true') ? console.log : function () {
};

module.exports = (function () {

    // Keep track of all the connections
    var connections = {};

    var sqlOptions = {
        parameterized: false,
        caseSensitive: true,
        escapeCharacter: '"',
        casting: true,
        canReturnValues: false,
        escapeInserts: true,
        declareDeleteAlias: false,
        limitKeyword: 'ROWS',
        escapeValue: "'"
    };

    var adapter = {

        //
        // TODO: make the exported thing an EventEmitter for when there's no callback.
        //
        emit: function (evName, data) {

            // temporary hack- should only be used for cases that would crash anyways
            // (see todo above- we still shouldn't throw, emit instead, hence this stub)
            if (evName === 'error') {
                throw data;
            }
        },

        // Which type of primary key is used by default
        pkFormat: 'integer',

        // Whether this adapter is syncable (yes)
        syncable: true,

        defaults: {
            host: 'localhost',
            port: 3050,
            user: 'SYSDBA',
            password: 'masterkey',
            schema: true,

            pool: true,
            connectionLimit: 5,
            waitForConnections: true
        },

        registerConnection: _registerConnection.configure(connections),
        teardown: _teardownConnection.configure(connections),


        // Direct access to query
        query: function (connectionName, collectionName, query, data, cb, connection) {

            if (_.isFunction(data)) {
                cb = data;
                data = null;
            }

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __QUERY__, cb);
            } else {
                __QUERY__(connection, cb);
            }

            function __QUERY__(connection, cb) {

                // Run query
                log('Firebird.query: ', query);

                if (data) connection.query(query, data, cb);
                else connection.query(query, cb);

            }
        },


        // Fetch the schema for a collection
        // (contains attributes and autoIncrement value)
        describe: function (connectionName, collectionName, cb, connection) {

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __DESCRIBE__, cb);
            } else {
                __DESCRIBE__(connection, cb);
            }

            function __DESCRIBE__(connection, cb) {

                var connectionObject = connections[connectionName];
                var collection = connectionObject.collections[collectionName];
                if (!collection) {
                    return cb(util.format('Unknown collection `%s` in connection `%s`', collectionName, connectionName));
                }
                var tableName = utils.escape(collectionName);

                var query =
                    ' select f.RDB$FIELD_NAME as "name",\n' +
                    '        f.RDB$DEFAULT_SOURCE as "source",\n' +
                    '        f.RDB$NULL_FLAG as "notNull",\n' +
                    '        fs.RDB$FIELD_TYPE as "type",\n' +
                    '        fs.RDB$FIELD_SUB_TYPE as "subType",\n' +
                    '        fs.RDB$FIELD_LENGTH as "length",\n' +
                    '        fs.RDB$FIELD_PRECISION as "precision",\n' +
                    '        fs.RDB$FIELD_SCALE as "scale",\n' +
                    '        ch.RDB$BYTES_PER_CHARACTER as "bytesPerCharacter"\n' +
                    '   from RDB$RELATION_FIELDS f\n' +
                    '        left join RDB$FIELDS fs on fs.RDB$FIELD_NAME = f.RDB$FIELD_SOURCE\n' +
                    '        left join RDB$CHARACTER_SETS ch on ch.RDB$CHARACTER_SET_ID = fs.RDB$CHARACTER_SET_ID\n' +
                    '  where RDB$RELATION_NAME=' + tableName;

                // Run query
                log('Firebird.describe:\n', query);

                connection.query(query, function __DESCRIBE__(err, schema) {

                    if (err) {
                        return cb(err);
                    }

                    if (!schema || schema.length === 0) {
                        return cb()
                    }

                    // remove trailing spaces
                    schema.forEach(function (attr) {
                        attr.name = attr.name.trim();
                    });

                    var pkQuery =
                        ' select i.rdb$index_name,\n' +
                        '        i.rdb$unique_flag,\n' +
                        '        i.rdb$index_inactive,\n' +
                        '        i.rdb$index_type,\n' +
                        '        isg.rdb$field_name as fieldName,\n' +
                        '        isg.rdb$field_position,\n' +
                        '        i.rdb$statistics,\n' +
                        '        i.rdb$expression_source,\n' +
                        '        c.RDB$CONSTRAINT_TYPE as "constraintType",\n' +
                        '        c.RDB$CONSTRAINT_NAME,\n' +
                        '        i.RDB$DESCRIPTION\n' +
                        '   from rdb$indices i\n' +
                        '        LEFT JOIN rdb$index_segments isg ON (isg.rdb$index_name = i.rdb$index_name)\n' +
                        '        LEFT JOIN rdb$relation_constraints c ON (i.rdb$index_name = c.rdb$index_name)\n' +
                        '  where i.rdb$relation_name = ' + tableName;

                    log('Firebird.describe (indices):\n', pkQuery);

                    connection.query(pkQuery, function (err, indices) {

                        if (err) {
                            return cb(err);
                        }

                        // Loop Through Indexes and Add Properties
                        indices.forEach(function (index) {

                            index.fieldName = index.fieldName.trim();
                            if (index.constraintType) {
                                index.constraintType = index.constraintType.toString();
                            }

                            schema.forEach(function (attr) {
                                if (attr.name !== index.fieldName) return;

                                attr.indexed = true;

                                if (index.constraintType === 'PRIMARY KEY') {
                                    attr.primaryKey = true;

                                    // If also an integer set auto increment attribute
                                    //if (attr.Type === 'int(11)') {
                                    //    attr.autoIncrement = true;
                                    //}
                                }

                                if (index.constraintType === 'UNIQUE') {
                                    attr.unique = true;
                                }
                            });
                        });

                        // Convert Firebird format to standard javascript object
                        var normalizedSchema = sql.normalizeSchema(schema);

                        // Set Internal Schema Mapping
                        collection.schema = normalizedSchema;

                        // TODO: check that what was returned actually matches the cache
                        cb(null, normalizedSchema);
                    });

                });
            }
        },

        // Create a new collection
        define: function (connectionName, collectionName, definition, cb, connection) {
            var self = this;

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __DEFINE__, cb);
            } else {
                __DEFINE__(connection, cb);
            }

            function __DEFINE__(connection, cb) {

                var connectionObject = connections[connectionName];
                var collection = connectionObject.collections[collectionName];
                if (!collection) {
                    return cb(util.format('Unknown collection `%s` in connection `%s`', collectionName, connectionName));
                }
                var tableName = utils.escapeId(collectionName);

                // Iterate through each attribute, building a query string
                var schema = sql.schema(tableName, definition);

                // Build query
                var query = 'CREATE TABLE ' + tableName + ' (' + schema + ')';

                if (connectionObject.config.charset) {
                    query += ' DEFAULT CHARSET ' + connectionObject.config.charset;
                }

                if (connectionObject.config.collation) {
                    if (!connectionObject.config.charset) query += ' DEFAULT ';
                    query += ' COLLATE ' + connectionObject.config.collation;
                }


                // Run query
                log('Firebird.define (table): ', query);

                connection.query(query, function __DEFINE__(err, result) {

                    if (err) return cb(err);

                    var autoIncKey = utils.autoIncrementKey(definition);

                    if (!autoIncKey) {
                        return __DESCRIBE__(err, result);
                    }

                    var genName = utils.generatorName(collectionName);

                    query = 'CREATE SEQUENCE ' + genName;

                    // Run query
                    log('Firebird.define (generator): ', query);

                    connection.query(query, function __SEQUENCE__(err, result) {

                        if (err) {
                            if (err.message.indexOf('already exists', 1) === -1) return next(err);
                            result = null;
                        }

                        var triggerName  = utils.incTriggerName(collectionName);

                        autoIncKey = utils.escapeId(autoIncKey);

                        query = 'CREATE TRIGGER ' + triggerName + ' for ' + tableName + '\n' +
                                'ACTIVE BEFORE INSERT POSITION 0\n' +
                                'AS BEGIN\n' +
                                '  IF (new.' + autoIncKey + ' is null) THEN\n' +
                                '    new.' + autoIncKey + ' = gen_id(' +  genName + ', 1);\n' +
                                'END';

                        log('Firebird.define (autoInc trigger): ', query);

                        connection.query(query, __DESCRIBE__);
                    });

                });

                function __DESCRIBE__(err, result) {
                    if (err) return cb(err);

                    self.describe(connectionName, collectionName, function (err) {
                        cb(err, result);
                    });
                }

            }
        },

        // Drop an existing collection
        drop: function (connectionName, collectionName, relations, cb, connection) {

            if (typeof relations === 'function') {
                cb = relations;
                relations = [];
            }

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __DROP__, cb);
            } else {
                __DROP__(connection, cb);
            }

            function __DROP__(connection, cb) {

                var connectionObject = connections[connectionName];


                // Drop any relations
                function dropTable(item, next) {

                    var tableName = utils.escapeId(collectionName);

                    // Build query
                    var query = 'DROP TABLE ' + tableName;

                    // Run query
                    log('Firebird.drop: ', query);

                    connection.query(query, function __DROP__(err, result) {
                        if (err) {
                            if (err.message.indexOf('error code = -607', 1) === -1) return next(err);
                            result = null;
                        }

                        next(null, result);
                    });
                }

                async.eachSeries(relations, dropTable, function (err) {
                    if (err) return cb(err);
                    dropTable(collectionName, cb);
                });

            }
        },

        //
        addAttribute: function (connectionName, collectionName, attrName, attrDef, cb, connection) {

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __ADD_ATTRIBUTE__, cb);
            } else {
                __ADD_ATTRIBUTE__(connection, cb);
            }

            function __ADD_ATTRIBUTE__(connection, cb) {

                var connectionObject = connections[connectionName];
                var collection = connectionObject.collections[collectionName];
                var tableName = collectionName;

                var query = sql.addColumn(tableName, attrName, attrDef);

                // Run query
                log('Firebird.addAttribute: ', query);

                connection.query(query, function (err, result) {
                    if (err) return cb(err);

                    // TODO: marshal response to waterline interface
                    cb(err);
                });

            }
        },

        //
        removeAttribute: function (connectionName, collectionName, attrName, cb, connection) {

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __REMOVE_ATTRIBUTE__, cb);
            } else {
                __REMOVE_ATTRIBUTE__(connection, cb);
            }

            function __REMOVE_ATTRIBUTE__(connection, cb) {

                var connectionObject = connections[connectionName];
                var collection = connectionObject.collections[collectionName];
                var tableName = collectionName;

                var query = sql.removeColumn(tableName, attrName);

                // Run query
                log('Firebird.removeAttribute: ', query);

                connection.query(query, function (err, result) {
                    if (err) return cb(err);

                    // TODO: marshal response to waterline interface
                    cb(err);
                });

            }
        },

        // No custom alter necessary-- alter can be performed by using the other methods (addAttribute, removeAttribute)
        // you probably want to use the default in waterline core since this can get complex
        // (that is unless you want some enhanced functionality-- then please be my guest!)

        // Create one or more new models in the collection
        create: function (connectionName, collectionName, data, cb, connection) {

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __CREATE__, cb);
            } else {
                __CREATE__(connection, cb);
            }

            function __CREATE__(connection, cb) {

                var connectionObject = connections[connectionName],
                    collection = connectionObject.collections[collectionName],
                    autoIncKey = utils.autoIncrementKey(collection.definition),
                    autoIncValue = null;

                if (!autoIncKey || data[autoIncKey]) {

                    __INSERT__()

                } else {

                    var genName = utils.generatorName(collectionName);

                    var query = 'SELECT GEN_ID(' + genName + ', 1) as "id" FROM RDB$DATABASE';

                    log('Firebird.create (generator): ', query);

                    return connection.query(query, function(err, result) {

                        if (err) return cb(handleQueryError(err));

                        autoIncValue = result[0].id;

                        __INSERT__();

                    });

                }

                function __INSERT__() {

                    if (autoIncValue) {
                        data[autoIncKey] = autoIncValue;
                    }

                    var _insertData = _.cloneDeep(data);

                    var schema = connectionObject.schema;
                    var _query;

                    var sequel = new Sequel(schema, sqlOptions);

                    // Build a query for the specific query strategy
                    try {
                        _query = sequel.create(collectionName, data);
                    } catch (e) {
                        return cb(e);
                    }


                    // Run query
                    log('Firebird.create (insert): ', _query.query);

                    connection.query(_query.query, function (err, result) {
                        if (err) return cb(handleQueryError(err));

                        var autoIncData = {};

                        if (autoIncKey) {
                            autoIncData[autoIncKey] = autoIncValue;
                        }

                        var values = _.extend({}, _insertData, autoIncData);
                        cb(err, values);
                    });
                }
            }
        },

        // Override of createEach to share a single connection
        // instead of using a separate connection for each request
        createEach: function (connectionName, collectionName, valuesList, cb, connection) {

            var connectionObject = connections[connectionName],
                collection = connectionObject.collections[collectionName],
                primaryKey = utils.primaryKey(collection.definition);

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __CREATE_EACH__, cb);
            } else {
                __CREATE_EACH__(connection, cb);
            }

            function __CREATE_EACH__(connection, cb) {

                var autoIncKey = utils.autoIncrementKey(collection.definition),
                    autoIncCount = 0,
                    autoIncValue;

                if (!autoIncKey) {
                    return __CREATE_EACH_ITEM__(connection, cb)
                } else {
                    _.each(valuesList, function (data) {
                        if (!data[autoIncKey]) autoIncCount++;
                    });
                }

                if (autoIncCount) {

                    var query = 'SELECT GEN_ID(' + genName + ', ' + autoIncCount + ') as "id" FROM RDB$DATABASE';

                    log('Firebird.createEach (generator): ', query);

                    return connection.query(query, function(err, result) {

                        if (err) return cb(handleQueryError(err));

                        autoIncValue = result[0].id - autoIncCount;

                        _.each(valueList, function(data) {
                            if (!data[autoIncKey]) {
                                data[autoIncKey] = autoIncValue;
                                autoIncValue++;
                            }
                        });

                        __INSERT__();

                    });

                } else {
                    return __CREATE_EACH_ITEM__(connection, cb)
                }

            }

            function __CREATE_EACH_ITEM__(connection, cb) {

                var tableName = collectionName;

                var records = [];

                async.eachSeries(valuesList,

                    function (data, cb) {

                        var schema = connectionObject.schema;
                        var _query;

                        var sequel = new Sequel(schema, sqlOptions);

                        // Build a query for the specific query strategy
                        try {
                            _query = sequel.create(collectionName, data);
                        } catch (e) {
                            return cb(e);
                        }

                        // Run query
                        log('Firebird.createEach: ', _query.query);

                        connection.query(_query.query, function (err, results) {
                            if (err) return cb(handleQueryError(err));
                            records.push(data[primaryKey]);
                            cb();
                        });
                    },

                    function (err) {

                        if (err) return cb(err);

                        // If there are no records (`!records.length`)
                        // then skip the query altogether- we don't need to look anything up
                        if (!records.length) {
                            return cb(null, []);
                        }

                        // Build a Query to get newly inserted records
                        var query = 'SELECT * FROM ' + utils.escapeId(tableName) + ' WHERE ' + utils.escapeId(primaryKey) + ' IN (' + records + ');';

                        // Run Query returing results
                        log('Firebird.createEach: ', query);

                        connection.query(query, function (err, results) {
                            if (err) return cb(err);
                            cb(null, results);
                        });
                    });

            }
        },

        /**
         * [join description]
         * @param  {[type]} conn     [description]
         * @param  {[type]} coll     [description]
         * @param  {[type]} criteria [description]
         * @param  {[type]} cb      [description]
         * @return {[type]}          [description]
         */
        join: function (connectionName, collectionName, options, cb, connection) {

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __JOIN__, cb);
            } else {
                __JOIN__(connection, cb);
            }

            function __JOIN__(client, done) {

                // Populate associated records for each parent result
                // (or do them all at once as an optimization, if possible)
                Cursor({

                    instructions: options,
                    nativeJoins: true,

                    /**
                     * Find some records directly (using only this adapter)
                     * from the specified collection.
                     *
                     * @param  {String}   collectionIdentity
                     * @param  {Object}   criteria
                     * @param  {Function} _cb
                     */
                    $find: function (collectionName, criteria, _cb) {
                        return adapter.find(connectionName, collectionName, criteria, _cb, client);
                    },

                    /**
                     * Look up the name of the primary key field
                     * for the collection with the specified identity.
                     *
                     * @param  {String}   collectionIdentity
                     * @return {String}
                     */
                    $getPK: function (collectionName) {
                        if (!collectionName) return;
                        return _getPK(connectionName, collectionName);
                    },

                    /**
                     * Given a strategy type, build up and execute a SQL query for it.
                     *
                     * @param {}
                     */

                    $populateBuffers: function populateBuffers(options, next) {

                        var buffers = options.buffers;
                        var instructions = options.instructions;

                        // Grab the collection by looking into the connection
                        var connectionObject = connections[connectionName];
                        var collection = connectionObject.collections[collectionName];

                        var parentRecords = [];
                        var cachedChildren = {};

                        // Grab Connection Schema
                        var schema = {};

                        Object.keys(connectionObject.collections).forEach(function (coll) {
                            schema[coll] = connectionObject.collections[coll].schema;
                        });

                        // Build Query
                        var _schema = connectionObject.schema;

                        var sequel = new Sequel(_schema, sqlOptions);
                        var _query;

                        // Build a query for the specific query strategy
                        try {
                            _query = sequel.find(collectionName, instructions);
                        } catch (e) {
                            return next(e);
                        }

                        async.auto({

                                processParent: function (next) {
                                    log('Firebird.populateBuffers: ', _query.query[0]);

                                    client.query(_query.query[0], function __FIND__(err, result) {
                                        if (err) return next(err);

                                        parentRecords = result;

                                        var splitChildren = function (parent, next) {
                                            var cache = {};

                                            _.keys(parent).forEach(function (key) {

                                                // Check if we can split this on our special alias identifier '___' and if
                                                // so put the result in the cache
                                                var split = key.split('___');
                                                if (split.length < 2) return;

                                                if (!hop(cache, split[0])) cache[split[0]] = {};
                                                cache[split[0]][split[1]] = parent[key];
                                                delete parent[key];
                                            });

                                            // Combine the local cache into the cachedChildren
                                            if (_.keys(cache).length > 0) {
                                                _.keys(cache).forEach(function (pop) {
                                                    if (!hop(cachedChildren, pop)) cachedChildren[pop] = [];
                                                    cachedChildren[pop] = cachedChildren[pop].concat(cache[pop]);
                                                });
                                            }

                                            next();
                                        };


                                        // Pull out any aliased child records that have come from a hasFK association
                                        async.eachSeries(parentRecords, splitChildren, function (err) {
                                            if (err) return next(err);
                                            buffers.parents = parentRecords;
                                            next();
                                        });
                                    });
                                },

                                // Build child buffers.
                                // For each instruction, loop through the parent records and build up a
                                // buffer for the record.
                                buildChildBuffers: ['processParent', function (next, results) {
                                    async.each(_.keys(instructions.instructions), function (population, nextPop) {

                                        var populationObject = instructions.instructions[population];
                                        var popInstructions = populationObject.instructions;
                                        var pk = _getPK(connectionName, popInstructions[0].parent);

                                        var alias = populationObject.strategy.strategy === 1 ? popInstructions[0].parentKey : popInstructions[0].alias;

                                        // Use eachSeries here to keep ordering
                                        async.eachSeries(parentRecords, function (parent, nextParent) {
                                            var buffer = {
                                                attrName: population,
                                                parentPK: parent[pk],
                                                pkAttr: pk,
                                                keyName: alias
                                            };

                                            var records = [];

                                            // Check for any cached parent records
                                            if (hop(cachedChildren, alias)) {
                                                cachedChildren[alias].forEach(function (cachedChild) {
                                                    var childVal = popInstructions[0].childKey;
                                                    var parentVal = popInstructions[0].parentKey;

                                                    if (cachedChild[childVal] !== parent[parentVal]) {
                                                        return;
                                                    }

                                                    // If null value for the parentVal, ignore it
                                                    if (parent[parentVal] === null) return;

                                                    records.push(cachedChild);
                                                });
                                            }

                                            if (records.length > 0) {
                                                buffer.records = records;
                                            }

                                            buffers.add(buffer);
                                            nextParent();
                                        }, nextPop);
                                    }, next);
                                }],


                                processChildren: ['buildChildBuffers', function (next, results) {

                                    // Remove the parent query
                                    _query.query.shift();

                                    async.each(_query.query, function (q, next) {

                                        var qs = '';
                                        var pk;

                                        if (!Array.isArray(q.instructions)) {
                                            pk = _getPK(connectionName, q.instructions.parent);
                                        }
                                        else if (q.instructions.length > 1) {
                                            pk = _getPK(connectionName, q.instructions[0].parent);
                                        }

                                        parentRecords.forEach(function (parent) {
                                            if (_.isNumber(parent[pk])) {
                                                qs += q.qs.replace('^?^', parent[pk]) + ' UNION ';
                                            } else {
                                                qs += q.qs.replace('^?^', '"' + parent[pk] + '"') + ' UNION ';
                                            }
                                        });

                                        // Remove the last UNION
                                        qs = qs.slice(0, -7);

                                        // Add a final sort to the Union clause for integration
                                        if (parentRecords.length > 1) {
                                            var addedOrder = false;

                                            function addSort(sortKey, sorts) {
                                                if (!sortKey.match(/^[0-9,a-z,A-Z$_]+$/)) {
                                                    return;
                                                }
                                                if (!addedOrder) {
                                                    addedOrder = true;
                                                    qs += ' ORDER BY ';
                                                }

                                                var direction = sorts[sortKey] === 1 ? 'ASC' : 'DESC';
                                                qs += sortKey + ' ' + direction;
                                            }

                                            if (!Array.isArray(q.instructions)) {
                                                _.keys(q.instructions.criteria.sort).forEach(function (sortKey) {
                                                    addSort(sortKey, q.instructions.criteria.sort);
                                                });
                                            }
                                            else if (q.instructions.length === 2) {
                                                _.keys(q.instructions[1].criteria.sort).forEach(function (sortKey) {
                                                    addSort(sortKey, q.instructions[1].criteria.sort);
                                                });
                                            }
                                        }

                                        log('Firebird.processChildren: ', qs);

                                        client.query(qs, function __FIND__(err, result) {
                                            if (err) return next(err);

                                            var groupedRecords = {};

                                            result.forEach(function (row) {

                                                if (!Array.isArray(q.instructions)) {
                                                    if (!hop(groupedRecords, row[q.instructions.childKey])) {
                                                        groupedRecords[row[q.instructions.childKey]] = [];
                                                    }

                                                    groupedRecords[row[q.instructions.childKey]].push(row);
                                                }
                                                else {

                                                    // Grab the special "foreign key" we attach and make sure to remove it
                                                    var fk = '___' + q.instructions[0].childKey;

                                                    if (!hop(groupedRecords, row[fk])) {
                                                        groupedRecords[row[fk]] = [];
                                                    }

                                                    var data = _.cloneDeep(row);
                                                    delete data[fk];
                                                    groupedRecords[row[fk]].push(data);
                                                }
                                            });

                                            buffers.store.forEach(function (buffer) {
                                                if (buffer.attrName !== q.attrName) return;
                                                var records = groupedRecords[buffer.belongsToPKValue];
                                                if (!records) return;
                                                if (!buffer.records) buffer.records = [];
                                                buffer.records = buffer.records.concat(records);
                                            });

                                            next();
                                        });
                                    }, function (err) {
                                        next();
                                    });

                                }]

                            },
                            function (err) {
                                if (err) return next(err);
                                next();
                            });

                    }

                }, done);
            }
        },


        // Find one or more models from the collection
        // using where, limit, skip, and order
        // In where: handle `or`, `and`, and `like` queries
        find: function (connectionName, collectionName, options, cb, connection) {

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __FIND__, cb);
            } else {
                __FIND__(connection, cb);
            }

            function __FIND__(connection, cb) {

                // Check if this is an aggregate query and that there is something to return
                if (options.groupBy || options.sum || options.average || options.min || options.max) {
                    if (!options.sum && !options.average && !options.min && !options.max) {
                        return cb(Errors.InvalidGroupBy);
                    }
                }

                var connectionObject = connections[connectionName];
                var collection = connectionObject.collections[collectionName];

                // Build find query
                var schema = connectionObject.schema;
                var _query;

                var sequel = new Sequel(schema, sqlOptions);

                // Build a query for the specific query strategy
                try {
                    _query = sequel.find(collectionName, options);
                } catch (e) {
                    return cb(e);
                }

                // Run query
                log('Firebird.find: ', _query.query[0]);

                connection.query(_query.query[0], function (err, result) {

                    if (err) return cb(err);

                    utils.readBlobData(result, function(err, result) {
                        if (err) return cb(err);
                        cb(null, result);
                    });

                });

            }
        },

        // Count one model from the collection
        // using where, limit, skip, and order
        // In where: handle `or`, `and`, and `like` queries
        count: function (connectionName, collectionName, options, cb, connection) {

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __COUNT__, cb);
            } else {
                __COUNT__(connection, cb);
            }

            function __COUNT__(connection, cb) {

                // Check if this is an aggregate query and that there is something to return
                if (options.groupBy || options.sum || options.average || options.min || options.max) {
                    if (!options.sum && !options.average && !options.min && !options.max) {
                        return cb(Errors.InvalidGroupBy);
                    }
                }

                var connectionObject = connections[connectionName];
                var collection = connectionObject.collections[collectionName];

                // Build find query
                var schema = connectionObject.schema;
                var _query;

                var sequel = new Sequel(schema, sqlOptions);

                // Build a count query
                try {
                    _query = sequel.count(collectionName, options);
                } catch (e) {
                    return cb(e);
                }

                // Run query
                log('Firebird.count: ', _query.query[0]);

                connection.query(_query.query[0], function (err, result) {
                    if (err) return cb(err);
                    // Return the count from the simplified query
                    cb(null, result[0].count);
                });
            }
        },

        // Stream one or more models from the collection
        // using where, limit, skip, and order
        // In where: handle `or`, `and`, and `like` queries
        stream: function (connectionName, collectionName, options, stream, connection) {

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __STREAM__);
            } else {
                __STREAM__(connection);
            }

            function __STREAM__(connection, cb) {

                var connectionObject = connections[connectionName];
                var collection = connectionObject.collections[collectionName];
                var tableName = collectionName;

                // Build find query
                var query = sql.selectQuery(tableName, options);

                // Run query
                log('Firebird.stream: ', query);

                var dbStream = connection.query(query);

                // Handle error, an 'end' event will be emitted after this as well
                dbStream.on('error', function (err) {
                    stream.end(err); // End stream
                    cb(err); // Close connection
                });

                // the field packets for the rows to follow
                dbStream.on('fields', function (fields) {
                });

                // Pausing the connnection is useful if your processing involves I/O
                dbStream.on('result', function (row) {
                    connection.pause();
                    stream.write(row, function () {
                        connection.resume();
                    });
                });

                // all rows have been received
                dbStream.on('end', function () {
                    stream.end(); // End stream
                    cb(); // Close connection
                });
            }
        },

        // Update one or more models in the collection
        update: function (connectionName, collectionName, options, values, cb, connection) {

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __UPDATE__, cb);
            } else {
                __UPDATE__(connection, cb);
            }

            function __UPDATE__(connection, cb) {

                var connectionObject = connections[connectionName];
                var collection = connectionObject.collections[collectionName];

                // Build find query
                var schema = connectionObject.schema;
                var _query;

                var sequel = new Sequel(schema, sqlOptions);

                // Build a query for the specific query strategy
                try {
                    _query = sequel.find(collectionName, _.cloneDeep(options));
                } catch (e) {
                    return cb(e);
                }

                log('Firebird.update(before): ', _query.query[0]);

                connection.query(_query.query[0], function (err, results) {
                    if (err) return cb(err);

                    var ids = [];

                    var pk = 'id';
                    Object.keys(collection.definition).forEach(function (key) {
                        if (!collection.definition[key].hasOwnProperty('primaryKey')) return;
                        pk = key;
                    });

                    // update statement will affect 0 rows
                    if (results.length === 0) {
                        return cb(null, []);
                    }

                    results.forEach(function (result) {
                        ids.push(result[pk]);
                    });

                    // Build query
                    try {
                        _query = sequel.update(collectionName, options, values);
                    } catch (e) {
                        return cb(e);
                    }

                    // Run query
                    log('Firebird.update: ', _query.query);

                    connection.query(_query.query, function (err, result) {
                        if (err) return cb(handleQueryError(err));

                        var criteria;
                        if (ids.length === 1) {
                            criteria = {where: {}, limit: 1};
                            criteria.where[pk] = ids[0];
                        } else {
                            criteria = {where: {}};
                            criteria.where[pk] = ids;
                        }

                        // Build a query for the specific query strategy
                        try {
                            _query = sequel.find(collectionName, criteria);
                        } catch (e) {
                            return cb(e);
                        }

                        // Run query
                        log('Firebird.update(after): ', _query.query[0]);

                        connection.query(_query.query[0], function (err, result) {
                            if (err) return cb(err);
                            cb(null, result);
                        });
                    });

                });
            }
        },

        // Delete one or more models from the collection
        destroy: function (connectionName, collectionName, options, cb, connection) {

            if (_.isUndefined(connection)) {
                return spawnConnection(connectionName, __DESTROY__, cb);
            } else {
                __DESTROY__(connection, cb);
            }

            function __DESTROY__(connection, cb) {

                var connectionObject = connections[connectionName];
                var collection = connectionObject.collections[collectionName];
                var tableName = collectionName;

                // Build query
                var schema = connectionObject.schema;

                var _query;

                var sequel = new Sequel(schema, sqlOptions);

                // Build a query for the specific query strategy
                try {
                    _query = sequel.destroy(collectionName, options);
                } catch (e) {
                    return cb(e);
                }

                async.auto({

                        findRecords: function (next) {
                            adapter.find(connectionName, collectionName, options, next, connection);
                        },

                        destroyRecords: ['findRecords', function (next) {
                            log('Firebird.destroy: ', _query.query);

                            connection.query(_query.query, next);
                        }]
                    },
                    function (err, results) {
                        if (err) return cb(err);
                        cb(null, results.findRecords);
                    });

            }
        },


        // Identity is here to facilitate unit testing
        // (this is optional and normally automatically populated based on filename)
        identity: 'sails-firebirdsql'
    };


    return adapter;


    /**
     * Wrap a function in the logic necessary to provision a connection.
     * (either grab a free connection from the pool or create a new one)
     *
     * cb is optional (you might be streaming), but... come to think of it...
     * TODO:
     * if streaming, pass in the stream instead of the callback--
     * then any relevant `error` events can be emitted on the stream.
     *
     * @param  {[type]}   connectionName
     * @param  {Function} fn
     * @param  {[type]}   cb
     */
    function spawnConnection(connectionName, fn, cb) {
        _spawnConnection(
            getConnectionObject(connectionName),
            fn,
            wrapCallback(cb)
        );
    }


    ////// NOTE /////////////////////////////////////////////////////////////
    //
    // both of these things should be done in WL core, imo:
    //
    // i.e.
    // getConnectionObject(connectionName)
    // wrapCallback(cb)
    //
    /////////////////////////////////////////////////////////////////////////


    /**
     * wrapCallback
     *
     * cb is optional (you might be streaming), but... come to think of it...
     * TODO:
     * if streaming, pass in the stream instead of the callback--
     * then any relevant `error` events can be emitted on the stream.
     *
     * @param  {Function} cb [description]
     * @return {[type]}      [description]
     */
    function wrapCallback(cb) {

        // Handle missing callback:
        if (!cb) {
            // Emit errors on adapter itself when no callback is present.
            cb = function (err) {
                try {
                    adapter.emit(STRINGFILE.noCallbackError + '\n' + err.toString());
                }
                catch (e) {
                    adapter.emit(err);
                }
            };
        }
        return cb;
    }


    /**
     * Lookup the primary key for the given collection
     * @param  {[type]} collectionIdentity [description]
     * @return {[type]}                    [description]
     * @api private
     */
    function _getPK(connectionIdentity, collectionIdentity) {

        var collectionDefinition;
        try {
            collectionDefinition = connections[connectionIdentity].collections[collectionIdentity].definition;

            return _.find(Object.keys(collectionDefinition), function _findPK(key) {
                    var attrDef = collectionDefinition[key];
                    if (attrDef && attrDef.primaryKey) return key;
                    else return false;
                }) || 'id';
        }
        catch (e) {
            throw new Error('Unable to determine primary key for collection `' + collectionIdentity + '` because ' +
                'an error was encountered acquiring the collection definition:\n' + require('util').inspect(e, false, null));
        }
    }


    /**
     *
     * @param  {String} connectionName
     * @return {Object} connectionObject
     */
    function getConnectionObject(connectionName) {

        var connectionObject = connections[connectionName];
        if (!connectionObject) {

            // this should never happen unless the adapter is being called directly
            // (i.e. outside of a CONNection OR a COLLection.)
            adapter.emit('error', Errors.InvalidConnection);
        }
        return connectionObject;
    }

    /**
     *
     * @param  {[type]} err [description]
     * @return {[type]}     [description]
     * @api private
     */
    function handleQueryError(err) {

        var formattedErr;

        // Check for uniqueness constraint violations:
        if (err.code === 'ER_DUP_ENTRY') {

            // Manually parse the Firebird error response and extract the relevant bits,
            // then build the formatted properties that will be passed directly to
            // WLValidationError in Waterline core.
            var matches = err.message.match(/Duplicate entry '(.*)' for key '(.*?)'$/);
            if (matches && matches.length) {
                formattedErr = {};
                formattedErr.code = 'E_UNIQUE';
                formattedErr.invalidAttributes = {};
                formattedErr.invalidAttributes[matches[2]] = [{
                    value: matches[1],
                    rule: 'unique'
                }];
            }
        }

        return formattedErr || err;
    }

})();

