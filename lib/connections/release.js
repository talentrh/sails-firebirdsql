/**
 * Module dependencies
 */

var Errors = require('waterline-errors').adapter;

/**
 * Functions for freeing/terminating a Firebird connection when a query is complete.
 *
 * @type {Object}
 */
module.exports = {

    /**
     * Frees the Firebird connection back into the pool.
     *
     * @param  {FirebirdConnection}   conn
     * @param  {Function} cb   [description]
     */
    poolfully: function (conn, cb) {
        if (!conn || typeof conn.detach !== 'function') {
            return cb(Errors.ConnectionRelease);
        }

        // Don't wait for connection release to trigger this callback.
        // (TODO: evaluate whether this should be the case)
        conn.detach();
        return cb();
    },


    /**
     * Terminates the Firebird connection.
     *
     * @param  {FirebirdConnection}   conn
     * @param  {Function} cb
     */
    poollessly: function (conn, cb) {
        if (!conn || typeof conn.end !== 'function') {
            return cb(Errors.ConnectionRelease);
        }

        // Wait for the connection to be ended, then trigger the callback.
        conn.end(cb);
    }
};
