mysql-dynamo-pump
=================

[![Build Status](https://david-dm.org/mpneuried/mysql-dynamo-pump.png)](https://david-dm.org/mpneuried/mysql-dynamo-pump)
[![NPM version](https://badge.fury.io/js/mysql-dynamo-pump.png)](http://badge.fury.io/js/mysql-dynamo-pump)

This is just an internal project to be able to pump data from a mysql database into AWS DynamoDB.
It's able to pumpe large ammounts of data as fast as your connection and dynamo write throughput allows.

It will not loose data if AWS returns your data by exceed the write throughput.
It tries to throttle and accelerate the pumping speed to find the maximum allowed write throughput.

All you have to do is overwrite the methods `readSQL()` and `convertToDynamoItem()`.

For an example have a look at the file `_src/example.coffee`