/**
 * Utility Functions
 */

// Dependencies
var Firebird = require('node-firebird');
var _ = require('lodash');
var async = require('async');
var url = require('url');

// Module Exports

var utils = module.exports = {};

/**
 * Parse URL string from config
 *
 * Parse URL string into connection config parameters
 */

utils.parseUrl = function (config) {
    if (!_.isString(config.url)) return config;

    var obj = url.parse(config.url);

    config.host = obj.hostname || config.host;
    config.port = obj.port || config.port;

    if (_.isString(obj.path)) {
        config.database = obj.path.split("/")[1] || config.database;
    }

    if (_.isString(obj.auth)) {
        config.user = obj.auth.split(":")[0] || config.user;
        config.password = obj.auth.split(":")[1] || config.password;
    }
    return config;
};

/**
 * Prepare values
 *
 * Transform a JS date to SQL date and functions
 * to strings.
 */

utils.prepareValue = function (value) {

    if (_.isUndefined(value) || value === null) return value;

    // Cast functions to strings
    if (_.isFunction(value)) {
        value = value.toString();
    }

    // Store Arrays and Objects as strings
    if (Array.isArray(value) || value.constructor && value.constructor.name === 'Object') {
        try {
            value = JSON.stringify(value);
        } catch (e) {
            // just keep the value and let the db handle an error
            value = value;
        }
    }

    // Cast dates to SQL
    if (_.isDate(value)) {
        value = utils.toSqlDate(value);
    }

    return this.escape(value);
};

utils.escape = function (value) {
    if (value instanceof Buffer) {
        return "x'" + value.toString('hex') + "'";
    }
    return Firebird.escape(value);
};

utils.escapeId = function (name) {
    if (name.length > 31) {
        name = name.substr(1, 31);
    }
    return '"' + name + '"';
};

utils.generatorName = function (tableName) {
    var name = 'gen_' + tableName;
    return utils.escapeId(name);
};

utils.autoIncrementKey = function (attr) {
    return _.findKey(attr, {autoIncrement: true});
};

utils.primaryKey = function (attr) {
    return _.findKey(attr, {primaryKey: true});
}

utils.incTriggerName = function (tableName) {
    var name = 'wl_bi_' + tableName;
    return utils.escapeId(name);
};

utils.readBlobData = function (rows, callback) {

    async.eachSeries(rows, function (row, doneRow) {

        var keys = [];

        _.forEach(row, function (value, key) {
            if (_.isFunction(value)) {
                keys.push(key);
            }
        });

        if (keys.length === 0) {
            return doneRow();
        }

        async.eachSeries(keys, function (key, doneKey) {
            var data = '';
            row[key](function (err, name, e) {
                if (err) return doneKey(err);

                e.on('data', function (chunk) {
                    data += chunk;
                });

                e.on('end', function () {
                    row[key] = data;
                    doneKey();
                });
            });
        }, function (err) {
            doneRow(err);
        });

    }, function (err) {
        if (err) return callback(err);
        callback(null, rows);
    });
}

/**
 * ignore
 */

utils.object = {};

/**
 * Safer helper for hasOwnProperty checks
 *
 * @param {Object} obj
 * @param {String} prop
 * @return {Boolean}
 * @api public
 */

var hop = Object.prototype.hasOwnProperty;
utils.object.hasOwnProperty = function (obj, prop) {
    return hop.call(obj, prop);
};


utils.toSqlDate = function toSqlDate(date) {
    var d = date.getFullYear() + '-' +
            ('00' + (date.getMonth() + 1)).slice(-2) + '-' +
            ('00' + date.getDate()).slice(-2),
        t = ('00' + date.getHours()).slice(-2) + ':' +
            ('00' + date.getMinutes()).slice(-2) + ':' +
            ('00' + date.getSeconds()).slice(-2);

    if (t === '00:00:00') {
        return d
    } else {
        return d + ' ' + t;
    }
};
