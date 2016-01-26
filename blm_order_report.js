
var connection = new Mongo();
var ravnDB = connection.getDB('ravn');
var mediaIDs = [];
var orders = new Object();

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
		// grab the info for the media ids
		//db['ravn.media'].find({'v.id':'0000994d-1544-4bf7-8e85-d58cc01fe860'},{'v.insight-metadata.barcode':1,'v.insight-metadata.title':1,'v.mediaType':1})
		var mediaObj = {};
		mediaObj['v.id'] = id;
		//var infoCursor = ravnDB['ravn.media'].find(mediaObj,{'v.insight-metadata.barcode':1,'v.insight-metadata.title':1,'v.mediaType':1})
		var infoCursor = ravnDB['ravn.media'].find(mediaObj)
		infoCursor.forEach( function(document) {
			var barcode = document.v['insight-metadata'].barcode;
			var type = document.v['mediaType'];
			
			orderObj = {};
			orderObj['barcode'] = barcode;
			orderObj['type'] = type;
			orderObj['frequency'] = 1;

			if(barcode in orders){
				orders[barcode].frequency += 1;
			} else {
				orders[barcode] = orderObj;
			}
			
		});
		
	});


	// sort descending
	var sorted = sortDescending(orders);

	print("Barcode\tFrequency\tType");
	
	sorted.forEach(function(ord){
		print(ord.barcode+"\t"+ord.frequency+"\t"+ord.type);
	})

	// for(var key in orders){
 	//  		print(orders[key].barcode+"\t"+orders[key].frequency+"\t"+orders[key].type);
	// }

	//print(orders['BLM00012361'].frequency);
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


function sortAscending(object){
	var sortable = [];
	for(var key in orders){
		sortable.push(object[key]);
	}
	
	var byFrequency = sortable.sort(function(a,b) {
    return a.frequency - b.frequency;
	});
	
	return byFrequency;
}

function sortDescending(object){
	var sortable = [];
	for(var key in orders){
		sortable.push(object[key]);
	}
	
	var byFrequency = sortable.sort(function(a,b) {
    return b.frequency - a.frequency;
	});
	
	return byFrequency;
}

generateReport();