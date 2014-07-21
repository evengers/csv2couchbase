# csv2couchbase 

> Import CSV files in Couchbase

Don't expect to much from this library. If you wish to see any other features - feel free to fork / contribute and push.


(Evengers) Note: this seems to work fine with Node 0.10.28  it chokes on 0.11.13


## Installation

1. Install [node.js](http://nodejs.org/).
1. Do `npm install csv2couchbase`.

## Usage

```
csv2couchbase --csv path/to/csv
              [--host hostname] 
              [--password password]
              [--bucket bucket]
              [--queryhost queryhost]
              [--columns "col1,col2,col3"]
              [--autocolumns]
              [--pick "col1,col3"]
              [--omit "col2"]
              [--chunksize 1000]
              [--doctype doctype]
              [--simulate]
```

## Command line options

* `--csv path/to/csv` - specify path to csv file
* `--host hostname`, `--password password`, `--bucket bucket`, `--queryhost queryhost` - see [coachbase](https://github.com/couchbase/couchnode) [documentation](http://www.couchbase.com/autodocs/couchbase-node-client-1.2.1/Connection.html)
* `--columns "col1,col2,col3"` - specify name of the columns in CSV file, in other case data will be imported as array of values.
* `--autocolumns` - set this flag is columns can be autodiscovered in the first CSV line.
* `--pick "col1,col3"` - specify which columns you want to include.
* `--omit "col2"` - other way of omitting columns which you don't want to import.
* `--chunksize` - default chunk size is 1000, script uses `addMulti` to import data by chunks.
* `--doctype` - specify `doctype` property for each object. Also this value will be used to construct id for rows `{doctype}_{index}`.
* `--simulate` - don't import anything to Couchbase, just simulate reading and print output to console.

## Example

If you want to import [GeoLite Country csv file](http://dev.maxmind.com/geoip/legacy/geolite/) 

```
csv2couchbase --csv ~/Desktop/GeoIPCountryWhois.csv --bucket default --doctype geoip --columns "ip_from,ip_to,ip_int_from,ip_int_to,country_short,country"
``` 
