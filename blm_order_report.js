
var connection = new Mongo();
var ravnDB = connection.getDB('ravn');
var mediaIDs = [];

function generateReport(){	
	var op = {};
	op['v.status'] = 'completed';
	var orderPart = ravnDB['ravn.orderpart'].find(op);
		
	orderPart.forEach( function(e) {
		var orderId = e.v['id'];
		
		var obj = {};
		obj['s'] = "ravn.orderpart."+orderId;
		obj['e'] = /ravn.media/;
	
		// use the order ID to find the mediaId in the link collection.
		collectMediaIds('link',obj)
	});


	mediaIDs.forEach(function(id){
		print(id);
	});
	
}

function collectMediaIds(collectionName,obj){
	var collection = ravnDB[collectionName].find(obj);
	if(collection.size() != 0){
		collection.forEach(function(document){
			// we either got a mediaid, or the order was for a dwf segment, so we need to some more processing for those orders.
			var mediaId = document['e'].replace(/ravn.media./g,'');
			mediaIDs.push(mediaId);
			});
	} else {
		// the asset ordered was a segment.
		processSegment(collectionName,obj);

	}
}

function processSegment(collectionName,obj){
	// use the order id to find the dwf-segment id
	
	obj['e'] = /ravn.dwf-segment/;
	ravnDB[collectionName].find(obj).forEach(function(document){
		var dwfSegmentId = document['e'];
		// // use the dwf-segment id to find the linked media id
		obj['s'] = dwfSegmentId;
		obj['e'] = /ravn.media/;
		ravnDB[collectionName].find(obj).forEach(function(document){
			var mediaId = document['e'].replace(/ravn.media./g,'');
			mediaIDs.push(mediaId);
		});
	});
}

generateReport();