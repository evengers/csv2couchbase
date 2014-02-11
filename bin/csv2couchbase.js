#!/usr/bin/env node
/*jshint globalstrict: true*/ 'use strict';

var _ = require('underscore'),
    nopt = require('nopt'),
    path = require('path'),
    csv = require('csv'),
    couchbase = require('couchbase');

// Parsing parameters
var parsed = nopt(
    {
      'host': [String, null],
      'password': [String, null],
      'bucket': [String, null],
      'queryhosts': [String, Array],
      'csv': path,
      'columns': [String],
      'autocolumns': [Boolean],
      'pick': [String, Array],
      'omit': [String, Array],
      'stream': [Boolean, false],
      'doctype': [String],
      'simulate': [Boolean]
    }
  );

var bucket = new couchbase.Connection({
  host: parsed.host,
  password: parsed.password,
  bucket: parsed.bucket,
  queryhosts: parsed.queryhosts
});

var data = {}, columns = parsed.columns ? parsed.columns.split(',') : null;

function constructObject(val) {
  if (parsed.pick) {
    val = _.pick.apply(_, _.union([val], parsed.pick));
  }
  if (parsed.omit) {
    val = _.omit.apply(_, _.union([val], parsed.omit));
  }
  if (parsed.doctype) {
    val['doctype'] = parsed.doctype;
  }
  return val;
}

function constructKey(index, row) {
  var key = '';
  if (parsed.doctype) {
    key += parsed.doctype + '_';
  }
  key += index;
  return key;
}

function errorHandler(err, result) {
  if (err) {
    console.error(err.message);
    process.exit(4);
  }
}

function printValue(key, obj) {
  console.log('Simulate: Key=' + key + ', Value=' + JSON.stringify(obj));
}

// Loading CSV
var csvStream = csv()
.from.path(
  parsed.csv, 
  { 
    columns: parsed.autocolumns || columns || null
  })
.transform(function(obj, index, cb) {
  var key = constructKey(index);
  var obj = constructObject(obj);
  if (!parsed.stream) {
    data[key] = { value: obj };
    cb(null, obj);
  } else {
    if (parsed.simulate) {
      printValue(key, obj);
      cb(null, obj);
    } else {
      bucket.set(key, obj, function(err, result) {
        errorHandler(err);
        cb(null, obj);
      });
    }
  }
})
.on('end', function(count){
  function reportSuccess() {
    console.log(count + ' rows imported');
    process.exit(0);
  }

  if (!parsed.stream) {
    if (parsed.simulate) {
      _(data).each(function(obj, key) {
        printValue(key, obj);
      });
      reportSuccess();
    } else {
      bucket.addMulti(data, {}, function(err, results) {
        errorHandler(err);
        reportSuccess();
      });
    }
  } else {
    reportSuccess();
  }
})
.on('error', errorHandler);
