# Duck DB ROS Plugin

This plugin alows you to read an import individual topics in ROS 1.0 bags into DuckDb as tables for analysis and export into other formats that DuckDb write (i.e. parquet). 

The scheme for the individual tables is generated from the embedded message definition in the ROS Bag itself. 

## Current Features

* Read bag information via `rosbag_info` command
* Read all topic messages into a table with `rosbag_scan` command
    * Schema is automatically generated based on the embedded ROS message type
* :fire:BLAZINGLY FAST:fire:
* Multi-file bag reading *not well tested*

## Future Features
* Arbitrary BLOB ROS message decoding 
* Tf2 Frame handling
* Ros2 Bag handling (maybe another plugin)


## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/ros/ros.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `ros.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Documentation for the functions added by the extenions are located in the [DuckDb Ros Function Reference](./docs/functions.md). The following illustrates a simple `robag_info` command: 

```
D select * from rosbag_info('../../CA-20190828184706_blur_align.bag');
┌──────────────────────┬─────────────────┬──────────────────────┬──────────────────────┬──────────┬────────┬──────────────────────┬──────────────────────┬────────────────────────────────────────┐
│      file_name       │    duration     │        start         │         end          │ messages │ chunks │     compression      │        types         │                 topics                 │
│       varchar        │    interval     │     timestamp_ns     │     timestamp_ns     │  uint64  │ uint64 │ struct("type" varc…  │ struct("name" varc…  │ struct("name" varchar, "type" varcha…  │
├──────────────────────┼─────────────────┼──────────────────────┼──────────────────────┼──────────┼────────┼──────────────────────┼──────────────────────┼────────────────────────────────────────┤
│ ../../CA-201908281…  │ 00:04:08.743266 │ 2019-08-29 01:47:0…  │ 2019-08-29 01:51:1…  │   103322 │   9980 │ [{'type': none, 'c…  │ [{'name': geometry…  │ [{'name': /camera_array/cam0/image_r…  │
└──────────────────────┴─────────────────┴──────────────────────┴──────────────────────┴──────────┴────────┴──────────────────────┴──────────────────────┴────────────────────────────────────────┘
```

Listing the first 5 available topics: 
```
select unnest(topics).name from rosbag_info('../../CA-20190828184706_blur_align.bag') LIMIT 5;
┌─────────────────────────────────────────┐
│         (unnest(topics))."name"         │
│                 varchar                 │
├─────────────────────────────────────────┤
│ /camera_array/cam0/image_raw/compressed │
│ /camera_array/cam1/image_raw/compressed │
│ /camera_array/cam2/image_raw/compressed │
│ /camera_array/cam3/image_raw/compressed │
│ /camera_array/cam4/image_raw/compressed │
└─────────────────────────────────────────┘
```

Extracting a topic to a table: 
```
D select * from rosbag_scan('../../CA-20190828184706_blur_align.bag', topic='/rslidar_points') limit 5;
┌──────────────────────┬────────────┬──────────────────────┬─────────────────┬────────┬───┬────────────┬──────────┬──────────────────────┬──────────┐
│     rx_timestamp     │ header.seq │     header.stamp     │ header.frame_id │ height │ … │ point_step │ row_step │         data         │ is_dense │
│     timestamp_ns     │   uint32   │     timestamp_ns     │     varchar     │ uint32 │   │   uint32   │  uint32  │         blob         │ boolean  │
├──────────────────────┼────────────┼──────────────────────┼─────────────────┼────────┼───┼────────────┼──────────┼──────────────────────┼──────────┤
│ 2019-08-29 01:47:0…  │      99571 │ 2019-08-29 01:47:0…  │ rslidar         │     32 │ … │         32 │    64896 │ \xF6\xDF(A\xE1k\xD…  │ false    │
│ 2019-08-29 01:47:0…  │      99572 │ 2019-08-29 01:47:0…  │ rslidar         │     32 │ … │         32 │    64896 │ \x00\x00\xC0\x7F\x…  │ false    │
│ 2019-08-29 01:47:0…  │      99573 │ 2019-08-29 01:47:0…  │ rslidar         │     32 │ … │         32 │    64896 │ \x8BX\x98@\xB7y<\x…  │ false    │
│ 2019-08-29 01:47:0…  │      99574 │ 2019-08-29 01:47:0…  │ rslidar         │     32 │ … │         32 │    64896 │ qw\x8B@R\xE0\x82\x…  │ false    │
│ 2019-08-29 01:47:0…  │      99575 │ 2019-08-29 01:47:0…  │ rslidar         │     32 │ … │         32 │    64896 │ \xFB$\xC1@_\xFF\x0…  │ false    │
├──────────────────────┴────────────┴──────────────────────┴─────────────────┴────────┴───┴────────────┴──────────┴──────────────────────┴──────────┤
│ 5 rows                                                                                                                       12 columns (9 shown) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL ros
LOAD ros
```
