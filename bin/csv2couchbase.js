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
      'chunksize': [Number],
      'doctype': [String],
      'simulate': [Boolean],
      'help': [Boolean]
    }
  );

if (!parsed.csv || parsed.help) {
  console.log(
    'Import CSV files in Couchbase\n' +
    '\n' +
    'Usage:\n' +
    '\tcsv2couchbase --csv path/to/csv\n' +
    '\t\t[--host hostname]\n' +
    '\t\t[--password password]\n' +
    '\t\t[--bucket bucket]\n' +
    '\t\t[--queryhost queryhost]\n' +
    '\t\t[--columns "col1,col2,col3"]\n' +
    '\t\t[--autocolumns]\n' +
    '\t\t[--pick "col1,col3"]\n' +
    '\t\t[--omit "col2"]\n' +
    '\t\t[--chunksize 1000]\n' +
    '\t\t[--doctype doctype]\n' +
    '\t\t[--simulate]');
  process.exit(1);
}

var bucket = parsed.simulate ? null : new couchbase.Connection({
  host: parsed.host,
  password: parsed.password,
  bucket: parsed.bucket,
  queryhosts: parsed.queryhosts
});

var data = {}, 
    currentSize = 0,
    chunkSize = parsed.chunksize || 1000,
    all = 0,
    columns = parsed.columns ? parsed.columns.split(',') : null;

function constructObject(val) {
  if (parsed.pick) {
    val = _.pick.apply(_, _.union([val], parsed.pick));
  }
  if (parsed.omit) {
    val = _.omit.apply(_, _.union([val], parsed.omit));
  }
  if (parsed.doctype) {
    val.doctype = parsed.doctype;
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

function flushChunk(cb) {
  var chunk = data;
  data = {};
  all += currentSize;
  currentSize = 0;
  bucket.addMulti(chunk, {}, function(err, results) {
    errorHandler(err);
    console.log('Imported ' + all + ' rows');
    cb();
  });
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
  var value = constructObject(obj);

  if (parsed.simulate) {
    printValue(key, obj);
    cb(null, obj);
  } else {
    data[key] = { value: value };
    currentSize++;
    if (currentSize >= chunkSize) {
      flushChunk(function() {
        cb(null, obj);
      });
    } else {
      cb(null, obj);
    }
  }
})
.on('end', function(count){
  function reportSuccess() {
    console.log(count + ' rows imported');
    process.exit(0);
  }

  if (currentSize > 0 && !parsed.simulate) {
    flushChunk(reportSuccess);
  } else {
    reportSuccess();
  }
})
.on('error', errorHandler);
