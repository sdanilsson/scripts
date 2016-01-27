var async = require('async')
var mongodb = require('mongodb');
var assert = require('assert');
var MongoClient = mongodb.MongoClient;
var url = 'mongodb://localhost:27017/ravn';


// we need to get information on all orders, the information on the assets ordered, and the users that ordered.

//var collection, db
var orderIds = [];
var mediaIds = [];
var segmentIds = [];

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
            var op = {};
            op['v.status'] = 'completed';

            var orderPart = db.collection('ravn.orderpart');

            orderPart.find(op, function(err, items) {
                items.count(function(err, count) {
                    var counter = 0;
                    items.forEach(function(item) {
                        if (err) callback(err);
                        var orderId = item.v['id'];
                        counter++;
                        //console.log("order " + orderId);
                        orderIds.push(orderId);
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
            var count = orderIds.length;
            var counter = 0;

            orderIds.forEach(function(orderId) {
                var query = {};
                query['s'] = "ravn.orderpart." + orderId;
                query['e'] = /ravn.media/;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var mediaId = item['e'].replace(/ravn.media./g, '');
                        //console.log(mediaId);
                        mediaIds.push(mediaId);
                    }
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
            var count = orderIds.length;
            var counter = 0;

            orderIds.forEach(function(orderId) {
                var query = {};
                query['s'] = "ravn.orderpart." + orderId;
                query['e'] = /ravn.dwf-segment/;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var mediaId = item['e'].replace(/ravn.dwf-segment./g, '');
                        //console.log(mediaId);
                        segmentIds.push(mediaId);
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
            var count = segmentIds.length;
            var counter = 0;

            segmentIds.forEach(function(segId) {
                var query = {};
                query['s'] = "ravn.dwf-segment." + segId;
                query['e'] = /ravn.media/;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var mediaId = item['e'].replace(/ravn.media./g, '');
                        //console.log(mediaId);
                        mediaIds.push(mediaId);
                    }

                    counter++;

                    if (counter === count) {
                        callback();
                    }
                });
            });
        },
        function(callback) {
            // Find the users attached to the order ids.
            var collection = db.collection('link');
            var count = orderIds.length;
            var counter = 0;

            orderIds.forEach(function(orderId) {
                var query = {};
                query['s'] = 'ravn.orderpart.' + orderId;
                query['e'] = /ravn.order/;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var order = item['e'].replace(/ravn.order./g, '');
                        console.log(order);
                    }

                    counter++;

                    if (counter === count) {
                        callback();
                    }

                });
            });

            // // Process the dwf-segment orders part 2, use the segmentIds array to find the linked media id.
            // var collection = db.collection('link');
            // var count = orderIds.length;
            // var counter = 0;

            // orderIds.forEach(function(oId) {
            //     var query = {};
            //     query['s'] = "ravn.orderpart." + oId;
            //     query['e'] = /ravn.order/;

            //     collection.find(query).nextObject(function(err, item) {
            //         if (item !== null) {
            //             var mediaId = item['e'].replace(/ravn.order./g, '');
            //             console.log(mediaId);
            //             //mediaIds.push(mediaId);
            //         }

            //         counter++;

            //         if (counter === count) {
            //             callback();
            //         }
            //     });
            // });

        }
        // function(callback){
        //     // Now we all the mediaIds, so we call look up the information for the asset that was ordered.
        //     //db['ravn.media'].find({'v.id':'0000994d-1544-4bf7-8e85-d58cc01fe860'},{'v.insight-metadata.barcode':1,'v.insight-metadata.title':1,'v.mediaType':1})

        //     var collection = db.collection('ravn.media');
        //     var count = mediaIds.length;
        //     var counter = 0;

        //     mediaIds.forEach(function(id){
        //         var query = {};
        //         query['v.id'] = id;

        //         collection.find(query).nextObject(function(err, item){
        //             var barcode = item.v['insight-metadata'].barcode;
        //             var type = item.v['mediaType'];

        //             console.log(barcode+" "+type);
        //             counter++;

        //             if(counter === count){
        //                 callback();
        //             }
        //         });

        //     });
        // }
    ],
    function(error, results) {
        console.log('disconnect from DB');
        error && console.error(error);
        db && db.close();
    });