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
      'pick': [String, Array],
      'omit': [String, Array],
      'stream': [Boolean, false],
      'doctype': [String]
    }
  );

var bucket = new couchbase.Connection({
  host: parsed.host,
  password: parsed.password,
  bucket: parsed.bucket,
  queryhosts: parsed.queryhosts
});

var data = {}, columns = parsed.columns ? parsed.columns.split(',') : null;

function constructObject(row) {
  var val = row;
  if (columns) {
    val = _.object(columns, val);
  }
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

// Loading CSV
var csvStream = csv()
.from.path(parsed.csv)
.transform(function(row, index, cb) {
   var key = constructKey(index);
  var obj = constructObject(row);
  if (!parsed.stream) {
    data[key] = { value: obj };
    cb(null, row);
  } else {
    bucket.set(key, obj, function(err, result) {
      errorHandler(err);
      cb(null, row);
    });
  }
})
.on('end', function(count){
  function reportSuccess() {
    console.log(count + ' rows imported');
    process.exit(0);
  }

  if (!parsed.stream) {
    bucket.addMulti(data, {}, function(err, results) {
      errorHandler(err);
      reportSuccess();
    });
  } else {
    reportSuccess();
  }
})
.on('error', errorHandler);
