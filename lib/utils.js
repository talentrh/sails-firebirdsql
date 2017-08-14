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
    return name;
};

utils.autoIncrementKey = function (attr) {
    return _.findKey(attr, function(item) {
      return item.autoIncrement ? true : false;
    });
};

utils.autoIncrementCustom = function (attr) {
    var customPk = false;
    _.findKey(attr, function(item) {
      if (!item.autoIncrement) return;
      var types = ['id_int'];
      customPk = _.includes(types, item.autoIncrement) ? item.autoIncrement: false;
    });

    return customPk;
};

utils.getPkKey = function (collection) {
  var pkKey;
  _.forEach(collection.definition, function(item, key) {
    if (item.primaryKey && !pkKey) {
      pkKey = key;
    }
  });
  return pkKey;
};

utils.findNewPk = function (connection, tableName, typePk, pkKey, cb) {
  tableName = tableName.toUpperCase();
  var selectPk = "SELECT FIRST(1) * FROM "+ tableName +" ORDER BY "+ pkKey +" DESC";

  connection.query(selectPk, function(err, result) {
    if (err) return cb(err);

    if (result.length) {
      var currentPk = _.first(result)[pkKey];
      currentPk = utils.incrementCustomPk(currentPk, typePk);
      cb(null, currentPk);
    } else {
      cb(null, 1);
    }
  });
};

utils.findOrCreateCustomPk = function (connection, tableName, typePk, cb) {
  tableName = tableName.toUpperCase();

  var selectPk = "SELECT FIRST(1) * FROM RHHORARIO ORDER BY IDHORARIO DESC";
  var selectPk = "select * from " + tableName + " where tabela = '" + tableName + "'";

  // Firebird.select pk in seqpimarykey
  connection.query(selectPk, function(err, result) {
    if (err) return cb(err);

    if (result.length) {
      var currentPk = _.first(result)[typePk];
      cb(null, currentPk);
    } else {
      var firstPk = utils.incrementCustomPk(null, typePk);
      var insertPk = "INSERT INTO seqprimarykey (tabela, chavevalor, "+ typePk +") values ('"+ tableName +"', '*', "+ firstPk +");"

      // Firebird.insert pk in seqpimarykey
      connection.query(insertPk, function(err, result) {
        cb(err, firstPk);
      });
    }
  });
};

utils.incrementCustomPk = function (currentPk, typePk) {
  var incrementByType = {
    id_int : function() {
      return (currentPk || currentPk === 0) ? (parseInt(currentPk) + 1) : 0;
    }
  };

  return incrementByType[typePk]();
};

utils.updateCustomPk = function (connection, newPk, typePk, tableName, cb) {
  var tableName = tableName.toUpperCase();
  var updatePk = "update seqprimarykey set "+ typePk +" = "+ newPk +" where tabela = '"+ tableName +"'";

  // Firebird.update new pk in seqpimarykey
  connection.query(updatePk, function(err, result) {
    cb(err, result);
  });
};

utils.primaryKey = function (attr) {
    return _.findKey(attr, {primaryKey: true});
};

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
          return async.setImmediate(doneRow);
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
