var async = require('async')
var mongodb = require('mongodb');
var assert = require('assert');
var MongoClient = mongodb.MongoClient;
var url = 'mongodb://localhost:27017/ravn';


// we need to get information on all orders, the information on the assets ordered, and the users that ordered.

var aggregate = [];

async.series([
        function(callback) {
            // Connect to the database
            console.log('Connecting to DB');
            MongoClient.connect(url, function(err, ref) {
                if (ref) db = ref;
                callback(err, ref);
            })
        },
        function(callback) {
            var orderPart = db.collection('ravn.orderpart');
            var query = {};
            query['v.status'] = 'completed';

            orderPart.find(query, function(err, items) {
                items.count(function(err, count) {
                    var counter = 0;
                    items.forEach(function(item) {
                        if (err) callback(err);
                        var orderPartId = item.v['id'];
                        counter++;
                        //console.log("order " + orderPartId);
                        var op = {};
                        op['order_part'] = orderPartId;
                        aggregate.push(op); 

                        if (counter === count) {
                            callback();
                        }
                    });
                });
            });
        },
        function(callback) {
            // Collect all the media ids.      
            var collection = db.collection('link');
            // find out how many ids we have, there is one per object in the aggregate list.
            var count = aggregate.length;
            var counter = 0;


            // now for the tricky part
            aggregate.forEach(function(op) {
                var query = {};
                query['s'] = "ravn.orderpart." + op.order_part;
                query['e'] = /ravn.media/;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var mediaId = item['e'].replace(/ravn.media./g, '');
                        //console.log(mediaId);
                        op['media_id'] = mediaId;
                        } //else {
                    //     console.log("this orderpart id must be a dwf-segment");
                    //     console.log(query['s']);
                    //     // no in fact it is neither... the id is both an ravn.order, and a ravn.user-segment.
                    //     // how should we deal with that?
                    //     //db['link'].find({'s':'ravn.orderpart.fd47cc6e-22bd-430d-a0c2-6fadfb434f47'}).pretty()
                    //     // that aside..
                    //     // the problem now is that we have an orderpart but it may or may not contain a media id.
                    //     // should we check if it is not a dwf-segment, if it is remove the object from the array?
                    // }
                    counter++;

                    if (counter === count) {
                        callback();
                    }
                });
            });
        },
        function(callback) {
            // Process the dwf segment orders part 1, get the ids and push them to the segmentIds array;
            var collection = db.collection('link');
            var count = aggregate.length;
            var counter = 0;

            aggregate.forEach(function(op) {
                var query = {};
                query['s'] = "ravn.orderpart." + op.order_part;
                query['e'] = /ravn.dwf-segment/;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var mediaId = item['e'].replace(/ravn.dwf-segment./g, '');
                        //console.log(mediaId);
                        op['segment_id'] = mediaId;
                    }
                    counter++;

                    if (counter === count) {
                        callback();
                    }
                });
            });
        },
        function(callback) {
            // Process the dwf-segment orders part 2, use the segmentIds array to find the linked media id.
            var collection = db.collection('link');
            var segmentIds = aggregate.filter(function(op){
                if(op.hasOwnProperty('segment_id')){
                    return op;
                }
            });
            var count = segmentIds.length;
            var counter = 0;

            segmentIds.forEach(function(op) {
                if (op.hasOwnProperty('segment_id')) {
                    var query = {};
                    query['s'] = "ravn.dwf-segment." + op.segment_id;
                    query['e'] = /ravn.media/;

                    collection.find(query).nextObject(function(err, item) {
                        if (item !== null) {
                            var mediaId = item['e'].replace(/ravn.media./g, '');
                            op['media_id'] = mediaId;
                        }

                        counter++;

                        if (counter === count) {
                            callback();
                        }
                    });
                }
            });
        },
        function(callback) {
            // Find the users attached to the order ids.
            // Part 1 find the order id -- we may want to store the ordernumber at some point.
            var collection = db.collection('link');
            var count = aggregate.length;
            var counter = 0;

            aggregate.forEach(function(op) {
                var query = {};
                query['s'] = 'ravn.orderpart.' + op.order_part;
                query['e'] = /ravn.order/;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var orderId = item['e'].replace(/ravn.order./g, '');
                        op['order_id'] = orderId;
                    }

                    counter++;

                    if (counter === count) {
                        callback();
                    }

                });
            });
        },
        function(callback) {
            //Part 2 look up the ravn.order in the link collection, to find the user id
            //db['link'].find({'s':'ravn.order.fc41e901-b8bb-43c1-9967-dc33a65d1476'}).pretty()
            var collection = db.collection('link');
            var count = aggregate.length;
            var counter = 0;

            aggregate.forEach(function(op) {
                var query = {};
                query['s'] = 'ravn.order.' + op.order_id;
                query['e'] = /ravn.user/;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var userId = item['e'].replace(/ravn.user./g, '');
                        //console.log(userId);
                        op['user_id'] = userId;
                    }

                    counter++;

                    if (counter === count) {
                        callback();
                    }

                });
            });

        },
        function(callback) {
            // Part 3 look up ravn.user in the link collection
            var collection = db.collection('ravn.user');
            var count = aggregate.length;
            var counter = 0;

            aggregate.forEach(function(op) {
                var query = {};
                query['v.id'] = op.user_id;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var username = item['v'].username;
                        op['user_name'] = username;    
                    }

                    counter++;

                    if (counter === count) {
                        callback();
                    }

                });

            });


        },
        function(callback){
            // Process the media ids and associate get barcode and mediatype to the order part.
            //db['ravn.media'].find({'v.id':'0000994d-1544-4bf7-8e85-d58cc01fe860'},{'v.insight-metadata.barcode':1,'v.insight-metadata.title':1,'v.mediaType':1})

            var collection = db.collection('ravn.media');
            var count = aggregate.length;
            var counter = 0;

            aggregate.forEach(function(op){
                 if(op.hasOwnProperty('media_id')){
                    console.log(op.media_id);   
                 }
                 
            });
            
            console.log(aggregate[1].order_part);

            //we have undefined mediaIds --- does that make any sense?

            // aggregate.forEach(function(op){
            //     var query = {};
            //     query['v.id'] = op.media_id;

            //     collection.find(query).nextObject(function(err, item){
            //         var barcode = item.v['insight-metadata'].barcode;
            //         var type = item.v['mediaType'];

            //         console.log(barcode+" "+type);
            //         counter++;

            //         if(counter === count){
            //             callback();
            //         }
            //     });

            // });
       }
    ],
    function(error, results) {
        console.log('disconnect from DB');
        error && console.error(error);
        db && db.close();
    });