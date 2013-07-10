cli = require("cli").enable('help', 'status')

# replace the local "./pump" to "mysql-dynamo-pump"
Pump = require( "./pump" )

cli.parse
	type: [ "t", "Type of data.", [ "power", "servo" ], "servo" ]
	limit: [ "l", "Limit count of data. `0` = unlimited", "NUMBER", 10 ]
	offset: [ "o", "Offset to read the sql data. `0` = unlimited", "NUMBER", "0" ]
	user: [ "u", "Reduce the imported data to a special user", "string" ]
	pumps: [ "p", "Count of parrallel pumpung processes", "number", 20 ]
	batchsize: [ "b", "Size of dynamo batch write size.", "number", 25 ]


Cnf = Pump.utils.readJSON( "config.json" )

pump = new ( class SettingsPump extends Pump
	
	sqlStatement: =>

		# construct the statement string and args array
		
		args = []
		stmt = [ "SELECT * FROM #{@opt.type}settings" ]
		stmtQ = []
		
		if @opt.user?
			stmtQ.push( "user_id = ?" )
			args.push( @opt.user )
			
		stmt.push "WHERE #{ stmtQ.join( "\nAND " ) }" if stmtQ.length

		stmt.push "ORDER BY id"

		if @opt.limit > 0
			stmt.push( "LIMIT ?" )
			args.push( @opt.limit )

		if @opt.offset > 0
			stmt.push( "OFFSET ?" )
			args.push( @opt.offset )

		return [ stmt.join( "\n" ), args ]

	_convertSetting: ( sett )=>
		# required check for retries
		if sett?.PutRequest?.Item
			return sett.PutRequest.Item

		# validate sql data
		if sett?.user_id is null
			return null

		# generate hash and range by type
		if @opt.type is "servo"
			_h = "S" + sett.user_id	
			_r = sett.devicetype_id.toString()
		else
			_h = "P" + sett.user_id	
			_r = sett.trainingelement_id + "-" + sett.devicetype_id + "-" + ( sett.sort or 0 )

		# generate id to check for doubles
		_id = _h + ":" + _r

		# check if a element has been processed allready and push it to the `duplicates` list.
		if @shared.processedIds.indexOf( _id ) >= 0
			@shared.duplicates.push( _id ) 
			return null

		# add the `_id` to the list of allready processed list
		@shared.processedIds.push _id
		
		# construct the dynamo item
		_con =
			_h: 
				S:  _h
			_r: 
				S: _r
		
		_con._u = 
			N: sett._u.toString()

		_con.user_id = 
			S: sett.user_id

		_con.devicetype_id = 
			N: sett.devicetype_id.toString()

		if @opt.type is "servo"

			_con.jsonServo = 
				S: sett.jsonServo

		else
			_con.jsonPower = 
				S: sett.jsonPower

			_con.sort = 
				N: sett.sort.toString()

			_con.trainingelement_id = 
				N: sett.trainingelement_id.toString()

		_con

)( Cnf )
		

cli.main ( args, options )->
	cli.debug ["\n\nSTART with CONFIG:","\n------------------------------\n", JSON.stringify( Cnf, true, 4 ), "\n------------------------------\n", "OPTIONS:\n", JSON.stringify( options, true, 4 ), "\n------------------------------\n"].join("")
	pump.run( options )
	return

