path = require "path"
fs = require "fs"

module.exports = 
	readJSON: ( filepath )=>
		_path = path.resolve( filepath )
		content = fs.readFileSync( _path )
		return JSON.parse( content )

