/// <reference path="../lib/node.d.ts" />
/// <reference path="../node_modules/data-beams/data-beams.d.ts" />

import dataBeams = require('data-beams');
import stream = require('stream');

import debug = require('./debug');


var dbgHeaderWrappedStream = debug('HeaderWrappedStream');

export class HeaderWrappedStream extends stream.Readable {

    _ended: boolean;
    _received: number;
    _push: (chunk: any, encoding?: string) => boolean;
    _data: NodeBuffer[];
    _readLimit: number;

    end: () => void;

    constructor(stream: stream.Readable, header?: NodeBuffer) {
        super();

        this._ended = false;
        this._received = 0;
        if (header && header.length) {
            this._data = [header];
        } else {
            this._data = [];
        }
        this._readLimit = 0;

        // Pipe the data to this
        var _ = this;
        function dataPipeHandler(data: NodeBuffer) {
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

    _checkEnd(): boolean {
        if (this._ended && this._data.length === 0) {
            this.push(null);
            dbgHeaderWrappedStream('Stream finished, requested %d bytes, pushing null.', this._readLimit);
            return true;
        }
        return false;
    }

    _sendData() {
        if (this._checkEnd()) {
            return;
        }

        var read = 0;
        var buffer: NodeBuffer;
        var remaining = Math.max(this._readLimit, 0);

        while (remaining && (buffer = this._data[0])) {
            if (buffer.length > remaining) {
                // cut up the buffer and put the rest back in queue
                this._data[0] = buffer.slice(remaining);
                buffer = buffer.slice(0, remaining);
            } else {
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
    }

    _read(size: number) {
        this._readLimit = size;
        this._sendData();
    }

}

var dbgStreamConnection = debug('StreamConnection');

enum StreamFlags {
    HasMessage  = 0b10000000,
    HasStream   = 0b01000000,
    HasCallback = 0b00100000,
    IsCallback  = 0b00010000  // Used when we callback through the callback
}

export class StreamConnection extends dataBeams.Connection {

    // TODO: Maybe add callback expiry for orphaned callbacks???
    //       Though, people should just write good code..
    Callbacks: { [id: number]: Function; }

    stream(message: Object, stream?: stream.Readable, callback?: Function): dataBeams.TransferOut {
        var hasMessage  = (message || 0)  && StreamFlags.HasMessage;
        var hasStream   = (stream || 0)   && StreamFlags.HasStream;
        var hasCallback = (callback || 0) && StreamFlags.HasCallback;

        var flags = hasMessage | hasStream | hasCallback;

        var headerLength = 1;
        var headerOffset = 0;

        if (hasCallback)
            headerLength += 4;

        var outMessage: string;
        var outMessageByteLength: number = 0;

        if (hasMessage) {
            // TODO: Add support for embedding streams in the JSON message
            headerLength += 4;
            outMessage = JSON.stringify(message);
            outMessageByteLength = Buffer.byteLength(outMessage, 'utf8');
        }

        var outBuffer: NodeBuffer;
        outBuffer = new Buffer(headerLength + outMessageByteLength);

        // Flags are written first
        outBuffer.writeUInt8(flags, headerOffset);
        headerOffset += 1;

        // Then callback
        if (hasCallback) {
            // Generate callback id
            var callbackId: number;
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
        } else {
            return this.sendBuffer(outBuffer);
        }
    }

}

export function streamBeam(connection: dataBeams.Connection): StreamConnection {

    function buildCallbackHandler(callbackId: number): Function {
        return function streamBeamsCallbackResponder() {
            var transferLength = 5;
            var writeOffset = 0;

            var flags = StreamFlags.HasCallback | StreamFlags.IsCallback;

            var argumentString: string;
            var argumentByteLength: number;
            if (arguments.length) {
                flags |= StreamFlags.HasMessage;
                var args: any[] = Array.prototype.slice.call(arguments);
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
        }
    }

    function streamBeamsOnTransfer(transfer: dataBeams.TransferIn) {
        // Handle transfers through the data-beams connection

        var hasMessage: boolean;
        var hasStream: boolean;
        var hasCallback: boolean;
        var isCallback: boolean;

        var flags: number = null;
        var callbackId: number = null;
        var messageByteLength: number = null;
        var streamStart: number = null; // The point where the message ends and the stream begins

        var transferBuffer: NodeBuffer = null;
        var readOffset: number = 0;
        // Handle the data of the transfer
        function streamBeamsHandleTransfer(data: NodeBuffer) {
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
                hasMessage = !!(flags & StreamFlags.HasMessage);
                hasStream = !!(flags & StreamFlags.HasStream);
                hasCallback = !!(flags & StreamFlags.HasCallback);
                isCallback = !!(flags & StreamFlags.IsCallback);
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
                var callbacks: { [id: number]: Function; };
                var callback: Function;
                if ((hasMessage && messageByteLength !== null) || (!hasMessage && callbackId !== null)) {
                    // Got all the data we needed to process, remove listeners.
                    streamBeamsRemoveListeners();

                    callbacks = (<StreamConnection>connection).Callbacks;
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
                    } else {
                        dbgStreamConnection('Handling callback %d, transfer %d', callbackId, transfer.id);
                        callback.call(connection);
                    }
                }
            } else {
                // Handle as message stream push
                var streamStart: number = null;
                if (streamStart === null && flags !== null) {
                    if (hasMessage) {
                        if (messageByteLength !== null) {
                            streamStart = readOffset + messageByteLength;
                        }
                    } else {
                        // Callback or stream
                        streamStart = readOffset;
                    }
                }

                if (streamStart !== null && transferBuffer.length >= streamStart) {
                    // Got all the data we needed to process, remove listeners.
                    streamBeamsRemoveListeners();

                    var messageObj: Object = null;
                    var callback: Function = null;
                    var stream: stream.Readable = null;

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
    (<StreamConnection>connection).Callbacks = {};

    return <StreamConnection>connection;
}
