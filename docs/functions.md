DuckDB Ros Function Reference

**[Table Functions](#table-functions)**

| Function | Summary |
| --- | --- |
| [rosbag_info](#rosbag_info) | Returns information about a ROS Bag or set of ROS Bags |
| [rosbag_scan](#rosbag_scan) | Read ROS Bag topic into a table | 

### rosbag_info

_Read the metadata from a ROS 1.0 bag (or set of bags) into a table._

#### Signature

```sql
rosbag_info (col0 VARCHAR)
rosbag_info (col0 VARCHAR[])
```

#### Description

RRead the metadata from the ros bag, including information about the bag size, topics and structure.

The `rosbag_info` table function accompanies the `rosbag_info` table function, but instead of reading the contents of a file, this function scans the metadata and the bag index inead. 

Since the data model of ROS bag quite flexible, most of the interesting metadata is within the returned `topics` or `types` columns, which is a somewhat complex nested structure of DuckDB `STRUCT` and `LIST` types.

#### Example

```sql
D select * from rosbag_info('../../CA-20190828184706_blur_align.bag');
┌──────────────────────┬─────────────────┬──────────────────────┬───┬────────┬──────────────────────┬──────────────────────┬──────────────────────┐
│      file_name       │    duration     │        start         │ … │ chunks │     compression      │        types         │        topics        │
│       varchar        │    interval     │     timestamp_ns     │   │ uint64 │ struct("type" varc…  │ struct("name" varc…  │ struct("name" varc…  │
├──────────────────────┼─────────────────┼──────────────────────┼───┼────────┼──────────────────────┼──────────────────────┼──────────────────────┤
│ ../../CA-201908281…  │ 00:04:08.743266 │ 2019-08-29 01:47:0…  │ … │   9980 │ [{'type': none, 'c…  │ [{'name': geometry…  │ [{'name': /camera_…  │
├──────────────────────┴─────────────────┴──────────────────────┴───┴────────┴──────────────────────┴──────────────────────┴──────────────────────┤
│ 1 rows                                                                                                                      9 columns (7 shown) │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql
--- Get a list of topic names in a bag
D select unnest(topics).name from rosbag_info('../../CA-20190828184706_blur_align.bag');
┌─────────────────────────────────────────┐
│         (unnest(topics))."name"         │
│                 varchar                 │
├─────────────────────────────────────────┤
│ /camera_array/cam0/image_raw/compressed │
│ /camera_array/cam1/image_raw/compressed │
│ /camera_array/cam2/image_raw/compressed │
│ /camera_array/cam3/image_raw/compressed │
│ /camera_array/cam4/image_raw/compressed │
│ /camera_array/cam5/image_raw/compressed │
│ /imu_raw                                │
│ /imu_rt                                 │
│ /mti/sensor/magnetic                    │
│ /navsat/fix                             │
│ /navsat/odom                            │
│ /novatel_data/bestpos                   │
│ /novatel_data/corrimudata               │
│ /novatel_data/inscov                    │
│ /novatel_data/inspvax                   │
│ /rslidar_points                         │
│ /ublox_gps_node/aidalm                  │
│ /ublox_gps_node/aideph                  │
│ /ublox_gps_node/fix                     │
│ /ublox_gps_node/fix_velocity            │
│ /ublox_gps_node/monhw                   │
│ /ublox_gps_node/navclock                │
│ /ublox_gps_node/navposecef              │
│ /ublox_gps_node/navpvt                  │
│ /ublox_gps_node/navsat                  │
│ /ublox_gps_node/navstatus               │
│ /ublox_gps_node/rxmraw                  │
│ /ublox_gps_node/rxmsfrb                 │
├─────────────────────────────────────────┤
│                 28 rows                 │
└─────────────────────────────────────────┘
```

### rosbag_scan

_The rosbag_scan() table function enables reading ROS 1.0 bag topics directly from a `.bag file.`_

#### Signature

```sql
rosbag_scan (col0 VARCHAR, topic VARCHAR)
rosbag_scan (col0 VARCHAR[], topic VARCHAR)
```

#### Description

The rosbag_scan() table function reads a topic directly from a ROS 1.0 `.bag file.` or set of bag files. 

This function uses multithreading and bag indexing which makes it a lot faster than using than most other import methods. Currently stats
are not handled by the index, so filtering is done after scanning. 

The output schema is determined by the message structure embedded in the bag itself. 

#### Example

```sql
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


