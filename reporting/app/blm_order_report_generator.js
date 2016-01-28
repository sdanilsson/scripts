var async = require('async')
var mongodb = require('mongodb');
var assert = require('assert');
var MongoClient = mongodb.MongoClient;
var url = 'mongodb://localhost:27017/ravn';


// we need to get information on all orders, the information on the assets ordered, and the users that ordered.

//var collection, db
var orderPartIds = [];
var mediaIds = [];
var segmentIds = [];
var orderIds = [];
var userIds = [];

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
                        var orderPartId = item.v['id'];
                        counter++;
                        //console.log("order " + orderPartId);
                        orderPartIds.push(orderPartId);
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
            var count = orderPartIds.length;
            var counter = 0;

            orderPartIds.forEach(function(orderPartId) {
                var query = {};
                query['s'] = "ravn.orderpart." + orderPartId;
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
            var count = orderPartIds.length;
            var counter = 0;

            orderPartIds.forEach(function(orderPartId) {
                var query = {};
                query['s'] = "ravn.orderpart." + orderPartId;
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
            // Part 1 find the order id -- we may want to store the ordernumber at some point.
            var collection = db.collection('link');
            var count = orderPartIds.length;
            var counter = 0;

            orderPartIds.forEach(function(orderPartId) {
                var query = {};
                query['s'] = 'ravn.orderpart.' + orderPartId;
                query['e'] = /ravn.order/;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var orderId = item['e'].replace(/ravn.order./g, '');
                        orderIds.push(orderId);
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
            var count = orderIds.length;
            var counter = 0;

            orderIds.forEach(function(orderId) {
                var query = {};
                query['s'] = 'ravn.order.' + orderId;
                query['e'] = /ravn.user/;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var user = item['e'].replace(/ravn.user./g, '');
                        userIds.push(user);
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
            var count = userIds.length;
            var counter = 0;

            userIds.forEach(function(userId) {
                var query = {};
                query['v.id'] = userId;
                
                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var user = item['v'].username;
                        console.log(user);
                    }

                    counter++;

                    if (counter === count) {
                        callback();
                    }

                });
            });


        } //, function(callback){
        //     // Part 4 finally look up the user name in the in ravn.user collection, and store it with the bacode asset/order object ... which I need to create.
        // }
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