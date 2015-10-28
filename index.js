var path = require('path'), 
	express = require('express'), 
	favicon = require('serve-favicon'), 
	logger = require('morgan'), 
	methodOverride = require('method-override'), 
	session = require('express-session'), 
	bodyParser = require('body-parser'), 
	errorHandler = require('errorhandler'),
	Passport = require('passport'), 
	ConnectionSource = require('cassandra-orm').ConnectionSource;

var app = express();
app.use(favicon(__dirname + '/public/assets/favicon.ico'));
app.use(logger('dev'));
app.use(methodOverride());
app.use(session({ 
	resave: true, 
	saveUninitialized: true, 
	secret: 'uwotm8' }
));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

var connOpt = {
		connection: {
			contactPoints: ['10.13.103.11'], 
			keyspace: 'microdataservice', 
			queryOptions: {consistency: ConnectionSource.consistencies.one}
		},
		options: {
			defaultReplicationStrategy : {
				class: 'SimpleStrategy', 
				replication_factor: 1
			},
			dropTableOnSchemaChange: false, 
			cache : true, 
		}
	}, 
	cDS = new ConnectionSource(connOpt);

cDS.connect(function(err, conn) {
	if(err) throw Error(err);

	app.use(cDS.setModelsDir(path.join(__dirname, 'models')));
	app.use(cDS.asMiddleware);

	var MScanner = require('ny').ModuleScanner;
	MScanner.scan(__dirname).apply(app);

	app.use(Passport.initialize());
	app.use(Passport.session());
})

var server = app.listen(8080, function () {
	var port = server.address().port;
	console.log('BPS Microdata server listening at http://localhost:%s', port);
});