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
var dbgHeaderWrappedStream = debug('HeaderWrappedStream');
var HeaderWrappedStream = (function (_super) {
    __extends(HeaderWrappedStream, _super);
    function HeaderWrappedStream(stream, header) {
        _super.call(this);
        this._ended = false;
        this._received = 0;
        if (header && header.length) {
            this._data = [header];
        }
        else {
            this._data = [];
        }
        this._readLimit = 0;
        // Pipe the data to this
        var _ = this;
        function dataPipeHandler(data) {
            _._data.push(data);
            _._received += data.length;
            dbgHeaderWrappedStream('Continuing data stream, added %d bytes (total %d).', data.length, _._received);
            _._sendData();
        }
        function removeDataPipeAndEnd() {
            dbgHeaderWrappedStream('Ending stream.');
            stream.removeListener('data', dataPipeHandler);
            stream.removeListener('end', removeDataPipeAndEnd);
            _._ended = true;
            // Send any remaining data up to the amount specified by _read if we can
            _._sendData();
        }
        stream.on('data', dataPipeHandler);
        stream.on('end', removeDataPipeAndEnd);
        this.end = removeDataPipeAndEnd;
    }
    HeaderWrappedStream.prototype._checkEnd = function () {
        if (this._ended && this._data.length === 0) {
            this.push(null);
            dbgHeaderWrappedStream('Stream finished, requested %d bytes, pushing null.', this._readLimit);
            return true;
        }
        return false;
    };
    HeaderWrappedStream.prototype._sendData = function () {
        if (this._checkEnd()) {
            return;
        }
        var read = 0;
        var buffer;
        var remaining = Math.max(this._readLimit, 0);
        while (remaining && (buffer = this._data[0])) {
            if (buffer.length > remaining) {
                // cut up the buffer and put the rest back in queue
                this._data[0] = buffer.slice(remaining);
                buffer = buffer.slice(0, remaining);
            }
            else {
                // Remove the buffer from the list
                this._data.shift();
            }
            // Send this buffer
            this.push(buffer);
            read += buffer.length;
            // Update the remaining amount of bytes we should read
            remaining = this._readLimit - read;
        }
        dbgHeaderWrappedStream('Reading data stream, requested %d bytes (provided %d).', this._readLimit, read);
        // Update the read limit
        this._readLimit -= read;
        this._checkEnd();
    };
    HeaderWrappedStream.prototype._read = function (size) {
        this._readLimit = size;
        this._sendData();
    };
    return HeaderWrappedStream;
})(stream.Readable);
exports.HeaderWrappedStream = HeaderWrappedStream;
var dbgStreamConnection = debug('StreamConnection');
var StreamFlags;
(function (StreamFlags) {
    StreamFlags[StreamFlags["HasMessage"] = 128] = "HasMessage";
    StreamFlags[StreamFlags["HasStream"] = 64] = "HasStream";
    StreamFlags[StreamFlags["HasCallback"] = 32] = "HasCallback";
    StreamFlags[StreamFlags["IsCallback"] = 16] = "IsCallback"; // Used when we callback through the callback
})(StreamFlags || (StreamFlags = {}));
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
    return StreamConnection;
})(dataBeams.Connection);
exports.StreamConnection = StreamConnection;
function streamBeam(connection) {
    function buildCallbackHandler(callbackId) {
        return function streamBeamsCallbackResponder() {
            var transferLength = 5;
            var writeOffset = 0;
            var flags = 32 /* HasCallback */ | 16 /* IsCallback */;
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
            // TODO: Add support for sending streams as arguments
            if (argumentString) {
                outBuffer.writeUInt32BE(argumentByteLength, writeOffset);
                writeOffset += 4;
                outBuffer.write(argumentString, writeOffset, argumentByteLength, 'utf8');
                writeOffset += argumentByteLength;
            }
            dbgStreamConnection('Sending callback response flags: %d, headerLength: %s, argsLength: %d', flags, outBuffer.length, arguments.length);
            // Finally send
            connection.sendBuffer(outBuffer);
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
            if (isCallback) {
                var callbacks;
                var callback;
                if ((hasMessage && messageByteLength !== null) || (!hasMessage && callbackId !== null)) {
                    // Got all the data we needed to process, remove listeners.
                    streamBeamsRemoveListeners();
                    callbacks = connection.Callbacks;
                    callback = callbacks[callbackId];
                    if (!callback)
                        throw new Error('Callback ' + callbackId + ' is not defined!');
                    // Delete the callback
                    // TODO: Add persistant callbacks?
                    delete callbacks[callbackId];
                    // Handle args if available
                    if (hasMessage) {
                        dbgStreamConnection('Handling callback %d, transfer %d, argsBytes %d', callbackId, transfer.id, messageByteLength);
                        var argsString = transferBuffer.toString('utf8', readOffset, readOffset + messageByteLength);
                        callback.apply(connection, JSON.parse(argsString));
                    }
                    else {
                        dbgStreamConnection('Handling callback %d, transfer %d', callbackId, transfer.id);
                        callback.call(connection);
                    }
                }
            }
            else {
                // Handle as message stream push
                var streamStart = null;
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
                if (streamStart !== null && transferBuffer.length >= streamStart) {
                    // Got all the data we needed to process, remove listeners.
                    streamBeamsRemoveListeners();
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
            transfer.removeListener('complete', streamBeamsRemoveListeners);
        }
        // Keep listening to process the full stream or json message
        transfer.on('data', streamBeamsHandleTransfer);
        transfer.on('complete', streamBeamsRemoveListeners);
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