var elastical = require('elastical')
  , generator = new (require('./mapping-generator'))
  , serialize = require('./serialize')
  , events = require('events')
  , _ = require('lodash')
  , async = require('async');

module.exports = function elasticSearchPlugin(schema, options) {
  var mapping = options && options.mapping || getMapping(schema)
    , indexName = options && options.index
    , typeName = options && options.type
    , alwaysHydrate = options && options.hydrate
    , defaultHydrateOptions = options && options.hydrateOptions
    , _mapping = null
    , host = options && options.host ? options.host : 'localhost'
    , port = options && options.port ? options.port : 9200
    , esClient = new elastical.Client(host, options)
    , useRiver = options && options.useRiver;

  if (useRiver)
    setUpRiver(schema);

  /**
   * Create the mapping. Takes a callback that will be called once
   * the mapping is created
   */
  schema.statics.createMapping = function (cb) {
    setIndexNameIfUnset(this.modelName);
    createMappingIfNotPresent(esClient, indexName, typeName, mapping, cb);
  };

  /**
   * @param indexName String (optional)
   * @param typeName String (optional)
   * @param callback Function
   */
  schema.methods.index = function (index, type, cb, options) {
    if (cb == null && typeof index == 'function') {
      cb = index;
      options = type;
      index = null;
      type = null;
    } else if (cb == null && typeof type == 'function') {
      cb = type;
      type = null;
      options = cb;
    }
    var model = this;
    setIndexNameIfUnset(model.constructor.modelName);
    options = options || {};
    options.id = options.id || model._id.toString();
    esClient.index(index || indexName, type || typeName, serialize(model, mapping), options, function (err, res) {
      model.emit('es-indexed', err, res);
      cb(err, res);
    });
  };


  /**
   * Unset elastic search index
   */
  schema.methods.unIndex = function () {
    var model = this;
    setIndexNameIfUnset(model.constructor.modelName);
    deleteByMongoId(esClient, model, indexName, typeName, 3);
  };

  schema.methods.saveAndIndex = function (refresh, cb) {
    var model = this;
    if (cb == null && typeof refresh == 'function') {
      cb = refresh;
      refresh = false;
    }
    model.save(function (err) {
      if (err) return cb(err);
      model.index(function (err, res) {
        if (err) console.log('index document failed because of: ', err, res, model.id);
        cb(err, model);
      }, {refresh: refresh});
    });
  };

  schema.methods.saveAndIndexSync = function (cb) {
    this.saveAndIndex(true, cb);
  };
  /**
   * Synchronize an existing collection
   *
   * @param callback - callback when synchronization is complete
   */
  schema.statics.synchronize = function (query, done) {
    if(done == null && typeof query === 'function'){
      done = query;
      query = {};
    }
    var model = this, bulkSize = 50;

    setIndexNameIfUnset(model.modelName);
    var start = new Date();
    model.count(query, function (err, totalCount) {
      if (err) return done && done(err);
      async.timesSeries(Math.ceil(totalCount / bulkSize),
        function (index, cb) {
          model.find(query).skip(index * bulkSize).limit(bulkSize).exec(function (err, docs) {
            if (err) return cb(err);
            esClient.bulk(_.map(docs, function (doc) {
              return {
                index: {
                  index: indexName,
                  type: typeName,
                  id: doc.id,
                  data: serialize(doc, mapping)
                }
              };
            }), function (err, res) {
              console.log('complete synchronize from ' + index * bulkSize + ' to ' + (index  + 1) * bulkSize);
              cb(err, res);
            });
          });
        },
        function (err) {
          var end = new Date();
          done && done(err, end.getTime() - start.getTime());
        });
    });
  };
  /**
   * ElasticSearch search function
   *
   * @param query - query object to perform search with
   * @param options - (optional) special search options, such as hydrate
   * @param callback - callback called with search results
   */
  schema.statics.search = function (query, options, cb) {
    var model = this;
    setIndexNameIfUnset(model.modelName);

    if (typeof options != 'object') {
      cb = options;
      options = {};
    }
    query.index = indexName;
    esClient.search(query, function (err, results, res) {
      if (err) {
        cb(err);
      } else {
        if (alwaysHydrate || options.hydrate) {
          hydrate(results, model, options.hydrateOptions || defaultHydrateOptions || {}, cb);
        } else {
          cb(null, res);
        }
      }
    });
  };

  schema.statics.recreateIndex = function (cb) {
    var model = this;
    console.log('to delete index:', indexName);
    esClient.deleteIndex(indexName, function (err) {
      if(err) return cb(err);
      model.createMapping(cb);
    });
  };

  schema.statics.esCount = function (query, cb) {
    var model = this;
    setIndexNameIfUnset(model.modelName);

    if (cb == null && typeof query === 'function') {
      cb = query;
      query = null;
    }
    var options = {index: indexName, type: typeName};
    if (query) {
      options.query = query;
    }
    esClient.count(options, cb);
  }

  schema.statics.clearAll = function (refresh, cb) {
    if (cb == null && typeof refresh == 'function') {
      cb = refresh;
      refresh = false;
    }

    var model = this;
    setIndexNameIfUnset(model.modelName);
    model.search({query: {match_all: {}}}, function (err, res) {
      if (res.hits.total > 0) {
        var bulkData = res.hits.hits.map(function (doc) {
          return {
            delete: {
              index: indexName,
              type: typeName,
              id: doc._id
            }
          };
        });
        esClient.bulk(bulkData, {refresh: refresh}, function (err, res) {
          cb(err, res);
        });
      } else cb(err, res);
    });
  };

  function setIndexNameIfUnset(model) {
    var modelName = model.toLowerCase();
    if (!indexName) {
      indexName = modelName + "s";
    }
    if (!typeName) {
      typeName = modelName;
    }
  }

  /*
   * Experimental MongoDB River functionality
   * NOTICE: Only tested with:
   *    MongoDB V2.4.1
   *    Elasticsearch V0.20.6
   *    elasticsearch-river-mongodb V1.6.5
   *      - https://github.com/richardwilly98/elasticsearch-river-mongodb/
   */
  function setUpRiver(schema) {
    schema.statics.river = function (cb) {
      var model = this;
      setIndexNameIfUnset(model.modelName);
      if (!this.db.name) throw "ERROR: " + model.modelName + ".river() call before mongoose.connect"
      esClient.putRiver(
        'mongodb',
        indexName,
        {
          type: 'mongodb',
          mongodb: {
            db: this.db.name,
            collection: indexName,
            gridfs: (useRiver && useRiver.gridfs) ? useRiver.gridfs : false
          },
          index: {
            name: indexName,
            type: typeName
          }
        }, cb);
    }
  }
};


function createMappingIfNotPresent(client, indexName, typeName, mapping, cb) {
  var completeMapping = {};
  completeMapping[typeName] = mapping;
  client.indexExists(indexName, function (err, exists) {
    if (exists) {
      console.log('index putMapping');
      client.putMapping(indexName, typeName, completeMapping, cb);
    } else {
      console.log('create new index');
      client.createIndex(indexName, {mappings: completeMapping}, cb);
    }
  });
}

function hydrate(results, model, options, cb) {
  var resultsMap = {};
  var ids = results.hits.map(function (a, i) {
    resultsMap[a._id] = i;
    return a._id;
  });
  var query = model.find({_id: {$in: ids}});

  // Build Mongoose query based on hydrate options
  // Example: {lean: true, sort: '-name', select: 'address name'}
  Object.keys(options).forEach(function (option) {
    query[option](options[option]);
  });

  query.exec(function (err, docs) {
    if (err) {
      return cb(err);
    } else {
      var hits = results.hits

      docs.forEach(function (doc) {
        var i = resultsMap[doc._id]
        hits[i] = doc
      })
      results.hits = hits;
      cb(null, results);
    }
  });
}
function getMapping(schema) {
  var retMapping = {};
  generator.generateMapping(schema, function (err, mapping) {
    retMapping = mapping;
  });
  return retMapping;
}
function deleteByMongoId(client, model, indexName, typeName, tries) {
  client.delete(indexName, typeName, model._id.toString(), function (err, res) {
    if (err && err.message.indexOf('404') > -1) {
      setTimeout(function () {
        if (tries <= 0) {
          // future issue.. what do we do!?
        } else {
          deleteByMongoId(client, model, indexName, typeName, --tries);
        }
      }, 500);
    } else {
      model.emit('es-removed', err, res);
    }
  });
}
