#!/usr/bin/env coffee;
var Cnf, Pump, SettingsPump, cli, pump, _ref,
  __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

cli = require("cli").enable('help', 'status');

Pump = require("./pump");

cli.parse({
  type: ["t", "Type of data.", ["power", "servo"], "servo"],
  limit: ["l", "Limit count of data. `0` = unlimited", "NUMBER", 10],
  offset: ["o", "Offset to read the sql data. `0` = unlimited", "NUMBER", "0"],
  user: ["u", "Reduce the imported data to a special user", "string"],
  pumps: ["p", "Count of parrallel pumpung processes", "number", 20],
  batchsize: ["b", "Size of dynamo batch write size.", "number", 25]
});

Cnf = Pump.utils.readJSON("config.json");

pump = new (SettingsPump = (function(_super) {
  __extends(SettingsPump, _super);

  function SettingsPump() {
    this._convertSetting = __bind(this._convertSetting, this);
    this.sqlStatement = __bind(this.sqlStatement, this);
    _ref = SettingsPump.__super__.constructor.apply(this, arguments);
    return _ref;
  }

  SettingsPump.prototype.sqlStatement = function() {
    var args, stmt, stmtQ;
    args = [];
    stmt = ["SELECT * FROM " + this.opt.type + "settings"];
    stmtQ = [];
    if (this.opt.user != null) {
      stmtQ.push("user_id = ?");
      args.push(this.opt.user);
    }
    if (stmtQ.length) {
      stmt.push("WHERE " + (stmtQ.join("\nAND ")));
    }
    stmt.push("ORDER BY id");
    if (this.opt.limit > 0) {
      stmt.push("LIMIT ?");
      args.push(this.opt.limit);
    }
    if (this.opt.offset > 0) {
      stmt.push("OFFSET ?");
      args.push(this.opt.offset);
    }
    return [stmt.join("\n"), args];
  };

  SettingsPump.prototype._convertSetting = function(sett) {
    var _con, _h, _id, _r, _ref1;
    if (sett != null ? (_ref1 = sett.PutRequest) != null ? _ref1.Item : void 0 : void 0) {
      return sett.PutRequest.Item;
    }
    if ((sett != null ? sett.user_id : void 0) === null) {
      return null;
    }
    if (this.opt.type === "servo") {
      _h = "S" + sett.user_id;
      _r = sett.devicetype_id.toString();
    } else {
      _h = "P" + sett.user_id;
      _r = sett.trainingelement_id + "-" + sett.devicetype_id + "-" + (sett.sort || 0);
    }
    _id = _h + ":" + _r;
    if (this.shared.processedIds.indexOf(_id) >= 0) {
      this.shared.duplicates.push(_id);
      return null;
    }
    this.shared.processedIds.push(_id);
    _con = {
      _h: {
        S: _h
      },
      _r: {
        S: _r
      }
    };
    _con._u = {
      N: sett._u.toString()
    };
    _con.user_id = {
      S: sett.user_id
    };
    _con.devicetype_id = {
      N: sett.devicetype_id.toString()
    };
    if (this.opt.type === "servo") {
      _con.jsonServo = {
        S: sett.jsonServo
      };
    } else {
      _con.jsonPower = {
        S: sett.jsonPower
      };
      _con.sort = {
        N: sett.sort.toString()
      };
      _con.trainingelement_id = {
        N: sett.trainingelement_id.toString()
      };
    }
    return _con;
  };

  return SettingsPump;

})(Pump))(Cnf);

cli.main(function(args, options) {
  cli.debug(["\n\nSTART with CONFIG:", "\n------------------------------\n", JSON.stringify(Cnf, true, 4), "\n------------------------------\n", "OPTIONS:\n", JSON.stringify(options, true, 4), "\n------------------------------\n"].join(""));
  pump.run(options);
});
