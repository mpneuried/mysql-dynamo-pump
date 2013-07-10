var AWS, MYSQL, Pump, async, cli, _,
  __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
  __slice = [].slice;

AWS = require("aws-sdk");

MYSQL = require("mysql");

async = require("async");

_ = require("lodash")._;

cli = require("cli");

Pump = (function() {
  function Pump(cnf) {
    this.cnf = cnf;
    this.writeProcess = __bind(this.writeProcess, this);
    this.updateProcess = __bind(this.updateProcess, this);
    this.pullSetting = __bind(this.pullSetting, this);
    this._convertSetting = __bind(this._convertSetting, this);
    this._createDynamoRequest = __bind(this._createDynamoRequest, this);
    this.writeBatch = __bind(this.writeBatch, this);
    this.accelerate = __bind(this.accelerate, this);
    this.throttle = __bind(this.throttle, this);
    this.pump = __bind(this.pump, this);
    this.startPumping = __bind(this.startPumping, this);
    this.readSettings = __bind(this.readSettings, this);
    this.sqlStatement = __bind(this.sqlStatement, this);
    this.testDynamoTable = __bind(this.testDynamoTable, this);
    this.initDynamo = __bind(this.initDynamo, this);
    this.initMySQL = __bind(this.initMySQL, this);
    this.onEnd = __bind(this.onEnd, this);
    this.middleware = __bind(this.middleware, this);
    this.run = __bind(this.run, this);
    return;
  }

  Pump.prototype.run = function(opt) {
    var _this = this;
    this.opt = opt;
    this.shared = {
      processedIds: [],
      duplicates: [],
      processedIds: [],
      iTodo: 0,
      iSuccess: 0
    };
    this.wait = 0;
    this.middleware(this.shared, this.initDynamo, this.testDynamoTable, this.initMySQL, this.readSettings, this.startPumping, function(err, shared) {
      if (err) {
        cli.error(JSON.stringify(err, true, 4));
        process.exit(0);
      } else {
        _this.onEnd(shared, function() {
          cli.ok("FINISHED!");
          process.exit(0);
        }, function(err) {
          cli.fatal(JSON.stringify(err, true, 4));
          process.exit(0);
        });
      }
    });
  };

  Pump.prototype.middleware = function() {
    var cb, data, fns, run, _error, _errorFn, _i;
    _error = false;
    fns = 2 <= arguments.length ? __slice.call(arguments, 0, _i = arguments.length - 1) : (_i = 0, []), cb = arguments[_i++];
    if (!_.isFunction(fns[0])) {
      data = fns.splice(0, 1)[0];
    } else {
      data = {};
    }
    _errorFn = function(error) {
      console.trace();
      fns = [];
      _error = true;
      cb(error, data);
    };
    run = function() {
      var fn;
      if (!fns.length) {
        return cb(null, data);
      } else {
        fn = fns.splice(0, 1)[0];
        return fn(data, function() {
          if (!_error) {
            run();
          }
        }, _errorFn, fns);
      }
    };
    run();
  };

  Pump.prototype.onEnd = function(shared, next, error) {
    var _ref;
    cli.ok("" + this.shared.iSuccess + " Dynamo entries written with " + (this.shared.iTodo - this.shared.iSuccess) + " retries");
    if ((_ref = this.shared.duplicates) != null ? _ref.length : void 0) {
      cli.info("Found " + this.shared.duplicates.length + " duplicate entries");
      cli.info(this.shared.duplicates);
    }
    console.timeEnd("Duration ");
    cli.debug("Tear down ... ");
    this.mysql.end();
    next();
  };

  Pump.prototype.initMySQL = function(shared, next, error) {
    this.mysql = MYSQL.createConnection(this.cnf.db);
    this.mysql.connect();
    next();
  };

  Pump.prototype.initDynamo = function(shared, next, error) {
    this.dynamo = new AWS.DynamoDB(this.cnf.aws);
    next();
  };

  Pump.prototype.testDynamoTable = function(shared, next, error) {
    var _this = this;
    this.dynamo.describeTable({
      TableName: this.cnf.aws.table
    }, function(err, data) {
      var _ref;
      if ((err != null) && err.statusCode !== 200) {
        error(err);
        return;
      }
      if ((data != null ? (_ref = data.Table) != null ? _ref.TableStatus : void 0 : void 0) === "ACTIVE") {
        cli.debug("Dynamo table `" + (data != null ? data.Table.TableName : void 0) + "` found and active. Current item count: " + (data != null ? data.Table.ItemCount : void 0));
        next();
      } else {
        error({
          message: "Dynamo table not active",
          name: "not-active"
        });
      }
    });
  };

  Pump.prototype.sqlStatement = function() {
    var _err;
    _err = new Error();
    _err.name = "missing-method-overwrite";
    _err.message = "You have to overwrite the method `sqlStatement()`. It should return an array of `[ sqlstatementString, argsArray ]`.";
    throw _err;
  };

  Pump.prototype.readSettings = function(shared, next, error) {
    var args, stmt, _ref,
      _this = this;
    cli.spinner('Read SQL data ...');
    _ref = this.sqlStatement(), stmt = _ref[0], args = _ref[1];
    this.mysql.query(stmt, args, function(err, data) {
      if (err) {
        error(err);
        return;
      }
      cli.spinner("" + data.length + " SQL entities loaded ...\n", true);
      cli.debug("LOADED " + data.length + " elements");
      shared.settings = data;
      shared.iTodo = data.length;
      shared.iDone = 0;
      next();
    });
  };

  Pump.prototype.startPumping = function(shared, next, error) {
    var aPumps, i,
      _this = this;
    cli.info("Start writing data to dynamo");
    console.time("Duration ");
    this.writeProcess();
    aPumps = (function() {
      var _i, _ref, _results;
      _results = [];
      for (i = _i = 1, _ref = this.opt.pumps; 1 <= _ref ? _i <= _ref : _i >= _ref; i = 1 <= _ref ? ++_i : --_i) {
        _results.push(this.pump);
      }
      return _results;
    }).call(this);
    async.parallel(aPumps, function(err, res) {
      next();
    });
  };

  Pump.prototype.pump = function(cba) {
    var _sett,
      _this = this;
    _sett = this.pullSetting();
    if (_sett.length > 0) {
      this.writeBatch(_sett, function(err) {
        _this.updateProcess(_sett.length);
        _.delay(_this.pump, _this.wait, cba);
      });
    } else {
      cba();
    }
  };

  Pump.prototype.throttle = function() {
    this.wait += 50;
    cli.debug("throttle " + this.wait);
  };

  Pump.prototype.accelerate = function() {
    if (this.wait - 50 <= 0) {
      this.wait = 0;
    } else {
      this.wait -= 50;
      cli.debug("accelerate " + this.wait);
    }
  };

  Pump.prototype.writeBatch = function(_settings, cb) {
    var _this = this;
    this.dynamo.batchWriteItem(this._createDynamoRequest(_settings), function(err, res) {
      var _putR, _ref, _ref1;
      if (_.isNumber(res != null ? (_ref = res.ConsumedCapacity) != null ? (_ref1 = _ref[0]) != null ? _ref1.CapacityUnits : void 0 : void 0 : void 0)) {
        _this.shared.iSuccess += res != null ? res.ConsumedCapacity[0].CapacityUnits : void 0;
      }
      if (!_.isEmpty(res != null ? res.UnprocessedItems : void 0)) {
        _this.throttle();
        _putR = res != null ? res.UnprocessedItems[_this.cnf.aws.table] : void 0;
        cli.debug("UnprocessedItems " + _putR.length);
        _this.shared.iTodo += _putR.length;
        _this.shared.settings = _this.shared.settings.concat(_putR);
        cb();
        return;
      } else {
        _this.accelerate();
      }
      if ((err != null) && err.statusCode !== 200) {
        cli.error(err);
        cb();
        return;
      }
      cb();
    });
  };

  Pump.prototype._createDynamoRequest = function(_settings) {
    var item, ret, sett, _dyn, _i, _len;
    ret = {
      RequestItems: {},
      ReturnConsumedCapacity: "TOTAL"
    };
    _dyn = [];
    for (_i = 0, _len = _settings.length; _i < _len; _i++) {
      sett = _settings[_i];
      item = this._convertSetting(sett);
      if (item != null) {
        _dyn.push({
          PutRequest: {
            Item: item
          }
        });
      }
    }
    ret.RequestItems[this.cnf.aws.table] = _dyn;
    return ret;
  };

  Pump.prototype._convertSetting = function(sett) {
    var _err;
    _err = new Error();
    _err.name = "missing-method-overwrite";
    _err.message = "You have to overwrite the method `_convertSetting( sett )` with your conversion of a sql entity object to a dynamo item object.";
    throw _err;
  };

  Pump.prototype.pullSetting = function() {
    return this.shared.settings.splice(0, this.opt.batchsize);
  };

  Pump.prototype.updateProcess = function(count) {
    this.shared.iDone += count;
    this.writeProcess();
  };

  Pump.prototype.writeProcess = function() {
    cli.progress(this.shared.iDone / this.shared.iTodo);
  };

  return Pump;

})();

Pump.utils = require('./utils');

module.exports = Pump;
