AWS = require "aws-sdk"
MYSQL = require "mysql"
async = require "async"
_ = require( "lodash" )._
cli = require("cli")

class Pump
	constructor: ( @cnf )->
		return

	run: ( @opt )=>
		
		@shared = 
			processedIds: []
			duplicates: []
			processedIds: []
			iTodo: 0
			iSuccess: 0

		@wait = 0

		@middleware(
			@shared,
			@initDynamo,
			@testDynamoTable,
			@initMySQL,
			@readSettings,
			@startPumping,
			( err, shared )=>
				
				if err
					cli.error JSON.stringify( err, true, 4 )
					process.exit(0)
				else
					@onEnd( shared, ->
						cli.ok( "FINISHED!" )
						process.exit(0)
						return
					, ( err )->
						cli.fatal JSON.stringify( err, true, 4 )
						process.exit(0)
						return
					)
				return
		)
		return

	middleware: =>
		_error = false
		[ fns..., cb ] = arguments
		
		if not _.isFunction( fns[ 0 ] )
			data = fns.splice( 0 , 1 )[ 0 ]
		else
			data = {}
		
		_errorFn = ( error )->
			console.trace()
			fns = []
			_error = true
			cb( error, data )
			return

		run = ->
			if not fns.length
				cb( null, data )
			else
				fn = fns.splice( 0 , 1 )[ 0 ]
				
				fn( data, ->
					run() if not _error
					return
				, _errorFn, fns )
		run()

		return

	onEnd: ( shared, next, error )=>
		cli.ok( "#{@shared.iSuccess} Dynamo entries written with #{ @shared.iTodo - @shared.iSuccess } retries" )

		if @shared.duplicates?.length
			cli.info "Found #{ @shared.duplicates.length } duplicate entries"
			cli.info @shared.duplicates

		console.timeEnd( "Duration " )

		cli.debug "Tear down ... "
		@mysql.end()
		next()
		return

	initMySQL: ( shared, next, error )=>
		@mysql = MYSQL.createConnection( @cnf.db )
		@mysql.connect()
		next()
		return

	initDynamo: ( shared, next, error )=>
		@dynamo = new AWS.DynamoDB( @cnf.aws )
		next()
		return

	testDynamoTable: ( shared, next, error )=>
		@dynamo.describeTable TableName: @cnf.aws.table, ( err, data )=>
			if err? and err.statusCode isnt 200
				error(err)
				return

			if data?.Table?.TableStatus is "ACTIVE"
				cli.debug "Dynamo table `#{data?.Table.TableName}` found and active. Current item count: #{ data?.Table.ItemCount }"
				next()
			else
				error( message: "Dynamo table not active", name: "not-active" )
			return

		return

	sqlStatement: =>
		_err = new Error()
		_err.name = "missing-method-overwrite"
		_err.message = "You have to overwrite the method `sqlStatement()`. It should return an array of `[ sqlstatementString, argsArray ]`."
		throw _err
		return

	readSettings: ( shared, next, error )=>

		cli.spinner('Read SQL data ...')

		[ stmt, args ] = @sqlStatement()

		@mysql.query stmt, args, ( err, data )=>
			if err
				error( err )
				return

			cli.spinner( "#{data.length} SQL entities loaded ...\n", true ) 
			cli.debug "LOADED #{data.length} elements"

			shared.settings = data
			shared.iTodo = data.length
			shared.iDone = 0
			next()
			return
		return

	startPumping: ( shared, next, error )=>
		

		cli.info "Start writing data to dynamo"
		console.time( "Duration " )
		@writeProcess()

		aPumps = for i in [ 1..@opt.pumps ]
			@pump

		async.parallel aPumps, ( err, res )=>
			next()
			return

		return

	pump: ( cba )=>
		_sett = @pullSetting()
		
		if _sett.length > 0
			@writeBatch _sett, ( err )=>
				@updateProcess( _sett.length )
				_.delay( @pump, @wait, cba )
				return
		else
			cba()
		return

	throttle: =>
		@wait += 50
		cli.debug( "throttle #{ @wait }" )
		return

	accelerate: =>
		if @wait - 50 <= 0
			@wait = 0
		else
			@wait -= 50
			cli.debug( "accelerate #{ @wait }" )
		return

	writeBatch: ( _settings, cb )=>
		
		@dynamo.batchWriteItem @_createDynamoRequest( _settings ), ( err, res )=>
			if _.isNumber( res?.ConsumedCapacity?[ 0 ]?.CapacityUnits )
				@shared.iSuccess += res?.ConsumedCapacity[ 0 ].CapacityUnits

			if not _.isEmpty( res?.UnprocessedItems )
				@throttle()
				_putR = res?.UnprocessedItems[ @cnf.aws.table ]
				cli.debug( "UnprocessedItems #{ _putR.length }" )
				@shared.iTodo += _putR.length
				@shared.settings = @shared.settings.concat( _putR )

				cb()
				return
			else
				@accelerate()

			if err? and err.statusCode isnt 200
				cli.error( err )
				cb( )
				return
			cb()
			return

		return

	_createDynamoRequest: ( _settings )=>
		ret = 
			RequestItems: {}
			ReturnConsumedCapacity: "TOTAL"
			#ReturnItemCollectionMetrics: "SIZE"

		_dyn = []

		for sett in _settings
			item = @_convertSetting( sett )
			if item?
				_dyn.push 
					PutRequest:
						Item: item

		ret.RequestItems[ @cnf.aws.table ] = _dyn

		ret

	_convertSetting: ( sett )=>
		_err = new Error()
		_err.name = "missing-method-overwrite"
		_err.message = "You have to overwrite the method `_convertSetting( sett )` with your conversion of a sql entity object to a dynamo item object."
		throw _err
		return

	pullSetting: =>
		@shared.settings.splice( 0, @opt.batchsize )


	updateProcess: ( count )=>
		@shared.iDone += count
		@writeProcess()
		return

	writeProcess: =>
		cli.progress( @shared.iDone / @shared.iTodo )
		return


Pump.utils = require './utils'

module.exports = Pump