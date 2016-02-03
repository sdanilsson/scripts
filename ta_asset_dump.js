var startDate = '2016-01-14T08:00:00.001Z';
var endDate   = '2016-02-01T12:00:00.000Z';

// var s = new Date();
// s.setHours(-12, -s.getTimezoneOffset(), 0, 0); //removing the timezone offset.
// startDate = s.toISOString();

// var e = new Date();
// e.setHours(12, -e.getTimezoneOffset(), 0, 0); //removing the timezone offset.
// endDate = e.toISOString(); 

var connection = new Mongo();
var ravnDB = connection.getDB('ravn');

ravnDB['ravn.media'].createIndex( { 'v.insight-metadata.dateIngested': 1 } )

var mediaCursor = ravnDB['ravn.media'].find(  {
	$and: [
		{ 'v.insight-metadata.dateIngested' : {$gte : new ISODate(startDate)}},
		{ 'v.insight-metadata.dateIngested' : {$lt  : new ISODate(endDate)}}
	]
}
);//.sort({'v.insight-metadata.dateIngested' : 1});

print("Date Ingested\tFile Name\tHRID\tMedia ID\tURL");

mediaCursor.forEach( function(e) {
	var insightMetadata = e.v['insight-metadata'];

	var d = insightMetadata.dateIngested;
	
	//convert to aus time
	var minuteOffset = d.getTimezoneOffset();
	var aedtMinuteOffset = 11*60; //11 = AEDT time (daylight) vs 10 = AEST (standard time)
	var dateDiff = minuteOffset + aedtMinuteOffset;
	d = new Date( d.getTime() + (dateDiff * 60 * 1000) );
	
	var assetLink = "https://insight.ta.mediarecall.com/discover/index#asset/" + e._id;

	print( 	d.toISOString() +
		'\t' + insightMetadata.customerMetadata.realFileName +
		'\t' + insightMetadata.hrid + 
		'\t' + e._id +
		'\t' + assetLink
	);
});


