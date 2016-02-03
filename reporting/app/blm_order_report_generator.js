var async = require('async')
var mongodb = require('mongodb');
var assert = require('assert');
var XLSX = require('xlsx');
var MongoClient = mongodb.MongoClient;
var url = 'mongodb://localhost:27017/ravn';



// barcode, media_type, order_type, user_name, ordernumber, date

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
            // Order number part 1, collect the order id (in order to get order number, and order date)
            var collection = db.collection('link');
            // find out how many ids we have, there is one per object in the aggregate list.
            var count = aggregate.length;
            var counter = 0;

            aggregate.forEach(function(op) {
                var query = {};
                query['s'] = "ravn.orderpart." + op.order_part;
                query['e'] = /ravn.order/;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var orderId = item['e'].replace(/ravn.order./g, '');
                        //console.log(orderId);
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
            // Order number part2, use the order id to look up the order number in orders.
            var collection = db.collection('ravn.order');
            // find out how many ids we have, there is one per object in the aggregate list.
            var orderIds = aggregate.filter(function(op) {
                if (op.hasOwnProperty('order_id')) {
                    return op;
                }
            });

            var counter = 0;
            var count = orderIds.length;

            orderIds.forEach(function(op) {
                var query = {};
                query['_id'] = op.order_id;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var orderNumber = item['v'].orderNo;
                        var orderDate = item['v'].creationDate;
                        op['order_no'] = orderNumber;
                        op['order_date'] = orderDate;
                    }

                    counter++;

                    if (counter === count) {
                        callback();
                    }
                });
            });
        },
        function(callback) {
            // Collect all the media ids for full asset orders     
            var collection = db.collection('link');
            // find out how many ids we have, there is one per object in the aggregate list.
            var count = aggregate.length;
            var counter = 0;


            aggregate.forEach(function(op) {
                var query = {};
                query['s'] = "ravn.orderpart." + op.order_part;
                query['e'] = /ravn.media/;

                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var mediaId = item['e'].replace(/ravn.media./g, '');
                        //console.log(mediaId);
                        op['media_id'] = mediaId;
                        op['order_type'] = 'full asset';
                    }

                    counter++;

                    if (counter === count) {
                        callback();
                    }
                });
            });
        },
        function(callback) {
            // Process any user-segment orders
            var collection = db.collection('link');
            var count = aggregate.length;
            var counter = 0;


            aggregate.forEach(function(op) {
                var query = {};
                query['s'] = "ravn.orderpart." + op.order_part;
                query['e'] = /ravn.user-segment/;


                collection.find(query).nextObject(function(err, item) {
                    if (item !== null) {
                        var userSegmentId = item['e'].replace(/ravn.user-segment./g, '');
                        //console.log(mediaId);
                        op['user_segment_id'] = userSegmentId;
                        op['order_type'] = 'user segment'
                    }

                    counter++;

                    if (counter === count) {
                        callback();
                    }
                });
            });
        },
        function(callback) {
            // Process user segment part 2 (only the objects that have a user_segment_id property)
            var collection = db.collection('link');
            var userSegmentIds = aggregate.filter(function(op) {
                if (op.hasOwnProperty('user_segment_id')) {
                    return op;
                }
            });

            var count = userSegmentIds.length;
            var counter = 0;

            aggregate.forEach(function(op) {
                var query = {};
                query['s'] = "ravn.user-segment." + op.user_segment_id;
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
                        var dwfSegmentId = item['e'].replace(/ravn.dwf-segment./g, '');
                        //console.log(dwfSegmentId);
                        op['dwf_segment_id'] = dwfSegmentId;
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
            var segmentIds = aggregate.filter(function(op) {
                if (op.hasOwnProperty('dwf_segment_id')) {
                    return op;
                }
            });
            var count = segmentIds.length;
            var counter = 0;

            segmentIds.forEach(function(op) {
                if (op.hasOwnProperty('dwf_segment_id')) {
                    var query = {};
                    query['s'] = "ravn.dwf-segment." + op.dwf_segment_id;
                    query['e'] = /ravn.media/;

                    collection.find(query).nextObject(function(err, item) {
                        if (item !== null) {
                            var mediaId = item['e'].replace(/ravn.media./g, '');
                            op['media_id'] = mediaId;
                            op['order_type'] = 'dwf segment';
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
        function(callback) {
            // Process the media ids and associate get barcode and mediatype to the order part.
            //db['ravn.media'].find({'v.id':'0000994d-1544-4bf7-8e85-d58cc01fe860'},{'v.insight-metadata.barcode':1,'v.insight-metadata.title':1,'v.mediaType':1})

            var collection = db.collection('ravn.media');
            var mediaIds = aggregate.filter(function(op) {
                if (op.hasOwnProperty('media_id')) {
                    return op;
                }
            });

            var count = mediaIds.length;
            var counter = 0;

            aggregate.forEach(function(op) {
                if (op.hasOwnProperty('media_id')) {
                    var query = {};
                    query['v.id'] = op.media_id;

                    collection.find(query).nextObject(function(err, item) {
                        var barcode = item.v['insight-metadata'].barcode;
                        var type = item.v['mediaType'];

                        op['barcode'] = barcode;
                        op['media_type'] = type;

                        counter++;

                        if (counter === count) {
                            callback();
                        }
                    });
                }
            });
        },
        function(callback) {
            // Create the excel file
            var data = [
                ['Barcode', 'Media type', 'Order type', 'User name', 'Order number', 'Date']
            ];

            // Load the data array with orders, and print orphaned orders to screen.    
            console.log('\tBarcode \tMedia type \tOrder type \tUser name \tOrder number \tDate');


            var sanitized = aggregate.filter(function(op) {
                if (op.user_name.indexOf('bydeluxe') === -1) {
                    return op;
                }
            });

            
            sanitized.forEach(function(op) {
                var row = [];
                var error = false;
                var errorRow = [];

                if (op.hasOwnProperty('barcode')) {
                    row.push(op.barcode);
                } else {
                    error = true;
                }

                if (op.hasOwnProperty('media_type')) {
                    row.push(op.media_type);
                } else {
                    error = true;
                }

                if (op.hasOwnProperty('order_type')) {
                    row.push(op.order_type);
                } else {
                    error = true;
                }

                if (op.hasOwnProperty('user_name')) {
                    row.push(op.user_name);
                } else {
                    error = true;
                }

                if (op.hasOwnProperty('order_no')) {
                    row.push(op.order_no);
                } else {
                    error = true;
                }

                if (op.hasOwnProperty('order_date')) {
                    row.push(op.order_date);
                } else {
                    error = true;
                }

                if (!error) {
                    data.push(row);
                } else {
                    console.log("Error:\t" + op.barcode + "\t" + op.media_type + "\t" + op.order_type + "\t" + op.user_name + "\t" + op.order_no + "\t" + op.order_date);
                }

            })

            var ws_name = "Orders";

            var wb = new Workbook();
            ws = sheet_from_array_of_arrays(data);

            wb.SheetNames.push(ws_name);
            wb.Sheets[ws_name] = ws;

            // modify a cell test
            // var mod_cell = ws['E1'];
            // mod_cell['v'] = 'test';

            XLSX.writeFile(wb, 'test.xlsx');

            callback();
        }
    ],
    function(error, results) {
        console.log('Disconnect from DB');
        error && console.error(error);
        db && db.close();
    });

function Workbook() {
    if (!(this instanceof Workbook)) return new Workbook();
    this.SheetNames = [];
    this.Sheets = {};
}

function sheet_from_array_of_arrays(data, opts) {
    var ws = {};
    var range = {
        s: {
            c: 10000000,
            r: 10000000
        },
        e: {
            c: 0,
            r: 0
        }
    };
    for (var R = 0; R != data.length; ++R) {
        for (var C = 0; C != data[R].length; ++C) {
            if (range.s.r > R) range.s.r = R;
            if (range.s.c > C) range.s.c = C;
            if (range.e.r < R) range.e.r = R;
            if (range.e.c < C) range.e.c = C;
            var cell = {
                v: data[R][C]
            };
            if (cell.v == null) continue;
            var cell_ref = XLSX.utils.encode_cell({
                c: C,
                r: R
            });

            if (typeof cell.v === 'number') cell.t = 'n';
            else if (typeof cell.v === 'boolean') cell.t = 'b';
            else if (cell.v instanceof Date) {
                cell.t = 'n';
                cell.z = XLSX.SSF._table[14];
                cell.v = datenum(cell.v);
            } else cell.t = 's';

            ws[cell_ref] = cell;
        }
    }
    if (range.s.c < 10000000) ws['!ref'] = XLSX.utils.encode_range(range);
    return ws;
}

function datenum(v, date1904) {
    if (date1904) v += 1462;
    var epoch = Date.parse(v);
    return (epoch - new Date(Date.UTC(1899, 11, 30))) / (24 * 60 * 60 * 1000);
}