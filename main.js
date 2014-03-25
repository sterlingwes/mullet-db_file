var _ = require('underscore'),
    fs = require('fs'),
    EJSON = require('meteor-ejson'),
    RSVP = require('rsvp');

module.exports = function(config) {
    
    var index = {},
        indexPath = [config.path,'store','index.json'].join('/');
    
    try {
        index = require( indexPath );
    }
    catch(e) { if(!e.code || e.code!='MODULE_NOT_FOUND')  console.error('! File DB: Could not parse index from FS', e); }
    
    // create directory if does not exist
    function forceDir(dirName, cb) {
        if(typeof cb !== 'function')
            cb = function(err) { };
        fs.mkdir([config.path,dirName].join('/'), cb);
    }
    
    function getDotVal(o,k) {
        if(!/\./.test(k))   return o[k];
        var path = k.split('.'),
            val = o;
        for(i=0; i<path.length; i++) {
            if(path[i] && val[path[i]])
                val = val[path][i];
        }
        return val;
    }
    
    // ensure store directory exists
    forceDir('store');
    
    /*
     * addIndex() - updates index after DB ops
     * 
     * - data, a Schema instance
     * - cb, callback
     */
    function addIndex(data, cb) {
        if(!data.name || !data.id)
            return console.error('! File DB: Cannot add index for unknown collection / id');
        
        if(!index[data.name])    index[data.name] = [];
        if(index[data.name].indexOf(data.id) ==-1)
            index[data.name].push(data.id);
        
        write(null, cb);
    }
    
    /*
     * removeIndex() - updates index after DB removal op
     * 
     * - data, a Schema instance or object with [id] and [name] keys
     * - cb, callback
     */
    function removeIndex(data, cb) {
        if(!data.name || !data.id)
            return console.error('! File DB: Cannot remove index for unknown collection / id');
        
        var arr = index[data.name],
            idx = arr.indexOf(data.id);
        if(arr && arr.length && idx!=-1)
            arr.splice(idx, 1);
        else if (arr && !arr.length)
            delete index[data.name];
        
        write(null, cb);
    }
    
    /*
     * write() - interface for database writes
     * 
     * - data, a Schema object instance
     * - cb, callback in node format, err or written instance of Schema
     */
    function write(data, cb) {
        
        if(!data) { // write index only, no data to write
            var indexJson;
            try {
                indexJson = JSON.stringify(index, null, ' ');
                fs.writeFile(indexPath,indexJson, function(err) { 
                    if(err) console.error('! File DB: Could not write index.'); 
                    if(!cb) cb = function() {};
                    cb(err);
                });
            } catch(e) { console.error('! File DB: Could not stringify index', e); }
            return;
        }
        
        if(!data.name || !data.id)
            return console.error('! File DB: Cannot write this object', data);
        
        var json;
        
        try{
            json = data.serialize({indent:' '});
        }
        catch(e) {
            cb(e);
        }
        
        if(!json)   return;
        
        forceDir('store/'+data.name, function(err) {
            fs.writeFile([ config.path, 'store', data.name, data.id+'.json' ].join('/'), json, function(err) {
                addIndex(data, function() {
                    cb(err, data);
                });
            });
        });
    }
    
    /*
     * read()
     * 
     * - name, name of schema to lookup
     * - selector, selector object
     * - cb, callback
     */
    function read(name, selector, cb) {
        
        var recIds = index ? index[name] || [] : []
          , items = {}
          , errors = [];
        
        if(typeof selector === 'string') // assume rec._id
            recIds = [selector];
        
        _.each(recIds, function(id) {
            try {
                items[id] = require( [config.path, 'store', name, id+'.json'].join('/') );
            }
            catch(e) {
                errors.push(e);
            }
        });
        
        if(typeof selector === 'object') {
            items = _.filter(items, function(item) {
                return _.every(selector, function(val,key) {
                    
                    if(key[0]=='$') { // treat as $or for now
                        return _.some(val, function(sel) { // one of the $or criteria needs to be met to pass
                            return _.every(sel, function(v,k) { // the whole criteria must pas
                                var itemVal = getDotVal(item,k);
                                return itemVal == v;
                            });
                        });
                    }
                    
                    if(typeof val === 'object') { // search within key val for more mongo operators
                        var keys = _.keys(val);
                        if(keys[0] && keys[0][0]=='$') {
                            var itemVal = getDotVal(item, key);
                            switch(keys[0]) {
                                case "$exists":
                                    return val[keys[0]] ? !itemVal : !!itemVal;
                                case "$in":
                                    return val[keys[0]].indexOf(itemVal) != -1;
                            }
                        }
                    }
                    return item[key] == val;
                });
            });
        }
        
        items = _.map(items, function(i) { return EJSON.fromJSONValue(i); });
        
        cb( errors.length ? errors : null, typeof selector === 'string' ? items[0] : items);
    }
    
    /*
     * remove() - interface for deleting records
     * 
     * - data, object: Schema instance representing a record, or an array of record ids, or a record id
     *                 must have a [name] key and either an [id] or [ids] key
     * - cb, callback
     */
    function remove(data, cb) {
        
        var ids = [];
        
        if(!cb) cb = function() {};
        
        if(data.id)
            ids.push(data.id);
        else if(data.ids && data.ids.length)
            Array.prototype.push.apply(ids, data);
        else
            Array.prototype.push.apply(ids, index[data.name]); // assume delete all
        
        var promises = [];
        
        _.each(ids, function(id) {
            promises.push(new RSVP.Promise(function(res,rej) {
                fs.unlink([ config.path, 'store', data.name, id+'.json' ].join('/'), function(err) {
                    removeIndex({ id: id, name: data.name }, function() {
                        if(err) rej(err);
                        else    res();
                    });
                });
            }));
        });
        
        RSVP.all(promises).then(function() {
            cb();
        })
        .catch(function(err) { console.error('Remove() error', err); cb(err); });
        
    }
    
    // API
    
    return {
        
        /*
         * find()
         * 
         * - name, name of the collection to search for
         * 
         * returns finder function to add to Schema as static method
         */
        find: function(name) {
            return function(data,cb) {
                if(!data || typeof data !== 'object' || !name)
                    return console.warn('! Invalid find operation. Requires valid Schema name & selector object.');

                read(name, data, cb);
            };
        },
        
        /*
         * insert()
         * 
         * - name, string name of the collection to insert into
         * 
         * returns insert function handler to add to Schema as static method
         */
        insert: function(name) {
            return function(data,cb) {
                if(!data || typeof data !== "object" || !data.name)
                    return console.warn('! Invalid insert operation. Requires valid Schema object.');

                write(data, cb);
            };
        },
        
        /*
         * remove()
         * 
         * - name, name of the collection to remove items from
         * 
         * returns remove function to add to Schema as static method
         */
        remove: function(name) {
            return function(data,cb) {
                if(!data || typeof data !== "object" || !name)
                    return console.warn('! Invalid remove operation. Requires valid Schema object or object with name and id or ids.');

                remove(_.extend({}, data, {name:name}), cb);
            };
        },
        
        _index: index
    };
    
};