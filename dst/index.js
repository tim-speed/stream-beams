/// <reference path="../lib/node.d.ts" />
/// <reference path="../node_modules/data-beams/data-beams.d.ts" />
var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
var dataBeams = require('data-beams');
var stream = require('stream');
var debug = require('./debug');
var dbgWrappedStream = debug('WrappedStream');
var WrappedStream = (function (_super) {
    __extends(WrappedStream, _super);
    function WrappedStream(stream, dataPipeHandler) {
        _super.call(this);
        // Pipe the data to this
        var _ = this;
        dataPipeHandler = dataPipeHandler || function dataPipeHandler(data) {
            _.write(data);
        };
        function removeDataPipeAndEnd() {
            dbgWrappedStream('Ending stream.');
            stream.removeListener('data', dataPipeHandler);
            stream.removeListener('end', removeDataPipeAndEnd);
            _.end();
        }
        stream.on('data', dataPipeHandler);
        stream.on('end', removeDataPipeAndEnd);
    }
    return WrappedStream;
})(dataBeams.ArrayBufferedStream);
exports.WrappedStream = WrappedStream;
var dbgHeaderWrappedStream = debug('HeaderWrappedStream');
var HeaderWrappedStream = (function (_super) {
    __extends(HeaderWrappedStream, _super);
    function HeaderWrappedStream(stream, header) {
        _super.call(this, stream);
        dbgHeaderWrappedStream('Initializing header-wrapped stream, header length %d bytes.', header.length);
        if (header.length) {
            this.write(header);
        }
    }
    return HeaderWrappedStream;
})(WrappedStream);
exports.HeaderWrappedStream = HeaderWrappedStream;
var dbgOutStreamManager = debug('OutStreamManager');
/**
 * OutStreamManager - Stream bridge, multiplexer, gets bytes written to it from program, builds
 *     and sends packets through data-beams transfer by writing to itself
 * Note: based on how it handles streams, only one per transfer should exist
 */
var OutStreamManager = (function (_super) {
    __extends(OutStreamManager, _super);
    function OutStreamManager() {
        _super.call(this);
        this.streamId = 0;
    }
    OutStreamManager.prototype.addStream = function (stream) {
        var streamId = ++this.streamId;
        dbgOutStreamManager('Adding stream %d for outbound transfer', streamId);
        // TODO: Potentially optimize?
        var header = new Buffer(5);
        header.writeUInt8(64 /* HasStream */, 0);
        header.writeUInt32BE(streamId, 1);
        // Define listeners
        var _ = this;
        function outStreamOnData(data) {
            // Write stream header + data packet
            _.write(Buffer.concat([header, data], 5 + data.length));
        }
        function outStreamOnEnd() {
            dbgOutStreamManager('Stream %d ended outbound transfer', streamId);
            // Cleanup
            stream.removeListener('data', outStreamOnData);
            stream.removeListener('end', outStreamOnEnd);
            // Write a message indicating that this stream has ended
            header.writeUInt8(64 /* HasStream */ | 8 /* CloseStream */, 0);
            _.write(header);
            // If this is the last stream to close end our transfer stream
            if (!--_.streamId) {
                _.end();
            }
        }
        // Register Data and End listeners
        stream.on('data', outStreamOnData);
        stream.on('end', outStreamOnEnd);
        return streamId;
    };
    return OutStreamManager;
})(dataBeams.ArrayBufferedStream);
var dbgInStreamManager = debug('InStreamManager');
/**
 * InStreamManager - Stream bridge, multiplexer, gets bytes written to it from program, builds
 *     and sends packets through individual streams that it builds
 * Note: based on how it handles streams, only one per transfer should exist
 */
var InStreamManager = (function () {
    function InStreamManager(transfer, initialPacket) {
        this.streams = {};
        this.transfer = transfer;
        // Define listeners
        var _ = this;
        function inTransferOnData(data) {
            // Parse header and route packet
            _._handleData(data);
        }
        function inTransferOnEnd() {
            // Cleanup
            transfer.removeListener('data', inTransferOnData);
            transfer.removeListener('end', inTransferOnEnd);
            for (var streamId in _.streams) {
                _._endStream(streamId);
            }
        }
        // Register Data and End listeners
        transfer.on('data', inTransferOnData);
        transfer.on('end', inTransferOnEnd);
        if (initialPacket && initialPacket.length)
            this._handleData(initialPacket);
    }
    InStreamManager.prototype._endStream = function (streamId) {
        dbgInStreamManager('Transfer %d ending stream %d', this.transfer.id, streamId);
        this.streams[streamId].end();
        delete this.streams[streamId];
    };
    InStreamManager.prototype._handleData = function (data) {
        // First byte is the flags
        var flags = data.readUInt8(0);
        // Next 4 is the id
        var streamId = data.readUInt32BE(1);
        if (flags & 8 /* CloseStream */) {
            this._endStream(streamId);
        }
        else {
            this.streams[streamId].write(data.slice(5));
        }
    };
    InStreamManager.prototype.buildStream = function (streamId) {
        var stream = this.streams[streamId] = new dataBeams.ArrayBufferedStream();
        dbgInStreamManager('Adding stream %d for inbound transfer', streamId);
        return stream;
    };
    return InStreamManager;
})();
var dbgStreamConnection = debug('StreamConnection');
var StreamFlags;
(function (StreamFlags) {
    StreamFlags[StreamFlags["HasMessage"] = 128] = "HasMessage";
    StreamFlags[StreamFlags["HasStream"] = 64] = "HasStream";
    StreamFlags[StreamFlags["HasCallback"] = 32] = "HasCallback";
    StreamFlags[StreamFlags["IsCallback"] = 16] = "IsCallback";
    StreamFlags[StreamFlags["CloseStream"] = 8] = "CloseStream"; // Used when there is multiple streams
})(StreamFlags || (StreamFlags = {}));
// TODO: This is a lazy way, should create a proper array struct using buffers
var STREAM_STRING_LEAD = '!@#$stream-beams-';
var StreamConnection = (function (_super) {
    __extends(StreamConnection, _super);
    function StreamConnection() {
        _super.apply(this, arguments);
    }
    StreamConnection.prototype.stream = function (message, stream, callback) {
        var hasMessage = (message || 0) && 128 /* HasMessage */;
        var hasStream = (stream || 0) && 64 /* HasStream */;
        var hasCallback = (callback || 0) && 32 /* HasCallback */;
        var flags = hasMessage | hasStream | hasCallback;
        var headerLength = 1;
        var headerOffset = 0;
        if (hasCallback)
            headerLength += 4;
        var outMessage;
        var outMessageByteLength = 0;
        if (hasMessage) {
            // TODO: Add support for embedding streams in the JSON message
            headerLength += 4;
            outMessage = JSON.stringify(message);
            outMessageByteLength = Buffer.byteLength(outMessage, 'utf8');
        }
        var outBuffer;
        outBuffer = new Buffer(headerLength + outMessageByteLength);
        // Flags are written first
        outBuffer.writeUInt8(flags, headerOffset);
        headerOffset += 1;
        // Then callback
        if (hasCallback) {
            // Generate callback id
            var callbackId;
            while ((callbackId = Math.floor(Math.random() * dataBeams.MAX_UINT_32)) && this.Callbacks[callbackId])
                continue;
            // Store the callback for later
            this.Callbacks[callbackId] = callback;
            outBuffer.writeUInt32BE(callbackId, headerOffset);
            headerOffset += 4;
        }
        // Then finally the message
        if (hasMessage) {
            outBuffer.writeUInt32BE(outMessageByteLength, headerOffset);
            headerOffset += 4;
            outBuffer.write(outMessage, headerOffset, outMessageByteLength, 'utf8');
            headerOffset += outMessageByteLength;
        }
        dbgStreamConnection('Sending message stream flags: %d, headerLength: %s, hasMessage: %d, hasStream: %d, hasCallback: %d', flags, outBuffer.length, hasMessage && 1, hasStream && 1, hasCallback && 1);
        if (hasStream) {
            return this.sendStream(new HeaderWrappedStream(stream, outBuffer));
        }
        else {
            return this.sendBuffer(outBuffer);
        }
    };
    StreamConnection.prototype._handleCallback = function (callbackId, messageBuffer, streamScraps, transfer) {
        var callbacks = this.Callbacks;
        var callback = callbacks[callbackId];
        if (!callback)
            throw new Error('Callback ' + callbackId + ' is not defined!');
        // Delete the callback
        // TODO: Add persistant callbacks?
        delete callbacks[callbackId];
        // Handle args if available
        if (messageBuffer) {
            // Process the callback message and handle any streams we find
            var argsString = messageBuffer.toString('utf8', 0, messageBuffer.length);
            dbgStreamConnection('Handling callback %d with args "%s" length %d', callbackId, argsString, messageBuffer.length);
            var args = JSON.parse(argsString);
            if (transfer) {
                var inStreamHandler = new InStreamManager(transfer, streamScraps);
                for (var i = 0; i < args.length; i++) {
                    var arg = args[i];
                    // TODO: This is a lazy way, should create a proper array struct using buffers
                    if (typeof arg === 'string' && arg.substr(0, STREAM_STRING_LEAD.length) === STREAM_STRING_LEAD) {
                        var streamId = Number(arg.substr(STREAM_STRING_LEAD.length));
                        args[i] = inStreamHandler.buildStream(streamId);
                    }
                }
            }
            callback.apply(this, args);
        }
        else {
            callback.call(this);
        }
    };
    return StreamConnection;
})(dataBeams.Connection);
exports.StreamConnection = StreamConnection;
function streamBeam(connection) {
    function buildCallbackHandler(callbackId) {
        return function streamBeamsCallbackResponder() {
            if (!connection.connected) {
                throw new Error('Cannot call callback, connection is in a disconnected state.');
            }
            var outStreamManager;
            var transferLength = 5;
            var writeOffset = 0;
            var flags = 32 /* HasCallback */ | 16 /* IsCallback */;
            for (var i = 0; i < arguments.length; i++) {
                var arg = arguments[i];
                if (arg instanceof stream.Readable) {
                    outStreamManager = outStreamManager || new OutStreamManager();
                    arguments[i] = STREAM_STRING_LEAD + outStreamManager.addStream(arg);
                }
            }
            if (outStreamManager) {
                flags |= 64 /* HasStream */;
            }
            var argumentString;
            var argumentByteLength;
            if (arguments.length) {
                flags |= 128 /* HasMessage */;
                var args = Array.prototype.slice.call(arguments);
                argumentString = JSON.stringify(args);
                argumentByteLength = Buffer.byteLength(argumentString, 'utf8');
                transferLength += 4 + argumentByteLength;
            }
            var outBuffer = new Buffer(transferLength);
            outBuffer.writeUInt8(flags, writeOffset);
            writeOffset += 1;
            outBuffer.writeUInt32BE(callbackId, writeOffset);
            writeOffset += 4;
            if (argumentString) {
                outBuffer.writeUInt32BE(argumentByteLength, writeOffset);
                writeOffset += 4;
                outBuffer.write(argumentString, writeOffset, argumentByteLength, 'utf8');
                writeOffset += argumentByteLength;
            }
            dbgStreamConnection('Sending callback response flags: %d, headerLength: %s, argsLength: %d, streams: %d', flags, outBuffer.length, arguments.length, (outStreamManager && outStreamManager.streamId) || 0);
            if (outStreamManager) {
                // Start a streaming transfer
                outStreamManager.write(outBuffer);
                connection.sendStream(outStreamManager);
            }
            else {
                // Send just the buffer, there are no streams
                connection.sendBuffer(outBuffer);
            }
        };
    }
    function streamBeamsOnTransfer(transfer) {
        // Handle transfers through the data-beams connection
        var hasMessage;
        var hasStream;
        var hasCallback;
        var isCallback;
        var flags = null;
        var callbackId = null;
        var messageByteLength = null;
        var streamStart = null; // The point where the message ends and the stream begins
        var transferBuffer = null;
        var readOffset = 0;
        // Handle the data of the transfer
        function streamBeamsHandleTransfer(data) {
            // Concat our buffer until we resolve out JSON message and stream
            if (!transferBuffer)
                transferBuffer = data;
            else
                transferBuffer = Buffer.concat([transferBuffer, data]);
            // Read flags if not read
            if (flags === null && transferBuffer.length >= 1) {
                flags = transferBuffer.readUInt8(readOffset);
                dbgStreamConnection('Reading flags from transfer %d : %d', transfer.id, flags);
                readOffset += 1;
                hasMessage = !!(flags & 128 /* HasMessage */);
                hasStream = !!(flags & 64 /* HasStream */);
                hasCallback = !!(flags & 32 /* HasCallback */);
                isCallback = !!(flags & 16 /* IsCallback */);
            }
            // Read callbackId if needed
            if (hasCallback && callbackId === null && transferBuffer.length >= readOffset + 4) {
                callbackId = transferBuffer.readUInt32BE(readOffset);
                dbgStreamConnection('Reading callback id from transfer %d : %d', transfer.id, callbackId);
                readOffset += 4;
            }
            // Read messageLength if needed
            if (hasMessage && messageByteLength === null && transferBuffer.length >= readOffset + 4) {
                messageByteLength = transferBuffer.readUInt32BE(readOffset);
                dbgStreamConnection('Reading message byte length from transfer %d : %d', transfer.id, messageByteLength);
                readOffset += 4;
            }
            if (streamStart === null && flags !== null) {
                if (hasMessage) {
                    if (messageByteLength !== null) {
                        streamStart = readOffset + messageByteLength;
                    }
                }
                else {
                    // Callback or stream
                    streamStart = readOffset;
                }
            }
            // Handle as message stream push
            dbgStreamConnection('Checking if stream start satisfied, %d of %d bytes received', transferBuffer.length, streamStart);
            if (streamStart !== null && transferBuffer.length >= streamStart) {
                // Got all the data we needed to process, remove listeners.
                streamBeamsRemoveListeners();
                if (isCallback) {
                    var callbacks;
                    var callback;
                    dbgStreamConnection('Handling callback %d, transfer %d, argsBytes %d', callbackId, transfer.id, messageByteLength || 0);
                    if (hasMessage) {
                        if (hasStream) {
                            connection._handleCallback(callbackId, transferBuffer.slice(readOffset, streamStart), transferBuffer.slice(streamStart), transfer);
                        }
                        else {
                            connection._handleCallback(callbackId, transferBuffer.slice(readOffset, streamStart));
                        }
                    }
                    else {
                        connection._handleCallback(callbackId);
                    }
                }
                else {
                    var messageObj = null;
                    var callback = null;
                    var stream = null;
                    if (hasMessage) {
                        dbgStreamConnection('Parsing message from transfer %d, length %d', transfer.id, messageByteLength);
                        var messageString = transferBuffer.toString('utf8', readOffset, streamStart);
                        messageObj = JSON.parse(messageString);
                    }
                    if (hasCallback) {
                        dbgStreamConnection('Building callback for transfer %d, callback %d', transfer.id, callbackId);
                        callback = buildCallbackHandler(callbackId);
                    }
                    if (hasStream) {
                        dbgStreamConnection('Parsing stream from transfer %d', transfer.id);
                        stream = new HeaderWrappedStream(transfer, transferBuffer.slice(streamStart));
                    }
                    // Emit!
                    connection.emit('message', messageObj, stream, callback);
                }
            }
        }
        function streamBeamsRemoveListeners() {
            dbgStreamConnection('Done with transfer %d', transfer.id);
            transfer.removeListener('data', streamBeamsHandleTransfer);
            transfer.removeListener('end', streamBeamsRemoveListeners);
        }
        // Keep listening to process the full stream or json message
        transfer.on('data', streamBeamsHandleTransfer);
        transfer.on('end', streamBeamsRemoveListeners);
    }
    connection.on('transfer', streamBeamsOnTransfer);
    // Stop listening for transfers
    connection.once('close', function streamBeamsOnClose() {
        connection.removeListener('transfer', streamBeamsOnTransfer);
    });
    // Convert connection to a StreamConnection
    connection.constructor = StreamConnection;
    connection.__proto__ = StreamConnection.prototype;
    // Init
    connection.Callbacks = {};
    return connection;
}
exports.streamBeam = streamBeam;
//# sourceMappingURL=index.js.map