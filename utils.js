var fs, path,
  _this = this;

path = require("path");

fs = require("fs");

module.exports = {
  readJSON: function(filepath) {
    var content, _path;
    _path = path.resolve(filepath);
    content = fs.readFileSync(_path);
    return JSON.parse(content);
  }
};
