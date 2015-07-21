/// <reference path="../lib/node.d.ts" />
/// <reference path="../node_modules/data-beams/data-beams.d.ts" />

import dataBeams = require('data-beams');
import stream = require('stream');

import debug = require('./debug');

var dbgWrappedStream = debug('WrappedStream');

export class WrappedStream extends dataBeams.ArrayBufferedStream {

    constructor(stream: stream.Readable, dataPipeHandler?: (data: Buffer) => void) {
        super();

        // Pipe the data to this
        var _ = this;
        dataPipeHandler = dataPipeHandler || function dataPipeHandler(data: Buffer) {
            _.write(data);
        }
        function removeDataPipeAndEnd() {
            dbgWrappedStream('Ending stream.');
            stream.removeListener('data', dataPipeHandler);
            stream.removeListener('end', removeDataPipeAndEnd);
            _.end();
        }
        stream.on('data', dataPipeHandler);
        stream.on('end', removeDataPipeAndEnd);
    }

}

var dbgHeaderWrappedStream = debug('HeaderWrappedStream');

export class HeaderWrappedStream extends WrappedStream {

    constructor(stream: stream.Readable, header: NodeBuffer) {
        super(stream);

        dbgHeaderWrappedStream('Initializing header-wrapped stream, header length %d bytes.', header.length);
        if (header.length) {
            this.write(header);
        }
    }

}

var dbgOutStreamManager = debug('OutStreamManager');

/**
 * OutStreamManager - Stream bridge, multiplexer, gets bytes written to it from program, builds
 *     and sends packets through data-beams transfer by writing to itself
 * Note: based on how it handles streams, only one per transfer should exist
 */
class OutStreamManager extends dataBeams.ArrayBufferedStream {

    streamId = 0;

    constructor() {
        super();
    }

    addStream(stream: stream.Readable): number {
        var streamId = ++this.streamId;
        dbgOutStreamManager('Adding stream %d for outbound transfer', streamId);
        // TODO: Potentially optimize?
        var header = new Buffer(5);
        header.writeUInt8(StreamFlags.HasStream, 0);
        header.writeUInt32BE(streamId, 1);
        // Define listeners
        var _ = this;
        function outStreamOnData(data: Buffer) {
            // Write stream header + data packet
            _.write(Buffer.concat([header, data], 5 + data.length));
        }
        function outStreamOnEnd() {
            dbgOutStreamManager('Stream %d ended outbound transfer', streamId);
            // Cleanup
            stream.removeListener('data', outStreamOnData);
            stream.removeListener('end', outStreamOnEnd);
            // Write a message indicating that this stream has ended
            header.writeUInt8(StreamFlags.HasStream | StreamFlags.CloseStream, 0);
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
    }

}

var dbgInStreamManager = debug('InStreamManager');

/**
 * InStreamManager - Stream bridge, multiplexer, gets bytes written to it from program, builds
 *     and sends packets through individual streams that it builds
 * Note: based on how it handles streams, only one per transfer should exist
 */
class InStreamManager {

    transfer: dataBeams.TransferIn;
    streams: { [id: number]: dataBeams.ArrayBufferedStream } = {};

    constructor(transfer: dataBeams.TransferIn, initialPacket?: Buffer) {
        this.transfer = transfer;
        // Define listeners
        var _ = this;
        function inTransferOnData(data: Buffer) {
            // Parse header and route packet
            _._handleData(data);
        }
        function inTransferOnEnd() {
            // Cleanup
            transfer.removeListener('data', inTransferOnData);
            transfer.removeListener('end', inTransferOnEnd);
            // End all streams
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

    _endStream(streamId: number) {
        dbgInStreamManager('Transfer %d ending stream %d', this.transfer.id, streamId);
        this.streams[streamId].end();
        delete this.streams[streamId];
    }

    _handleData(data: Buffer) {
        // First byte is the flags
        var flags = data.readUInt8(0);
        // Next 4 is the id
        var streamId = data.readUInt32BE(1);
        if (flags & StreamFlags.CloseStream) {
            this._endStream(streamId);
        } else {
            this.streams[streamId].write(data.slice(5));
        }
    }

    buildStream(streamId: number): dataBeams.ArrayBufferedStream {
        var stream = this.streams[streamId] = new dataBeams.ArrayBufferedStream();
        dbgInStreamManager('Adding stream %d for inbound transfer', streamId);
        return stream;
    }

}

var dbgStreamConnection = debug('StreamConnection');

enum StreamFlags {
    HasMessage  = 0b10000000,  // Used when packet relates to a message
    HasStream   = 0b01000000,  // Used when packet relates to a stream
    HasCallback = 0b00100000,  // Used when packet relates to a callback
    IsCallback  = 0b00010000,  // Used when we callback through the callback
    CloseStream = 0b00001000   // Used when there is multiple streams
}

// TODO: This is a lazy way, should create a proper array struct using buffers
var STREAM_STRING_LEAD = '!@#$stream-beams-';

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

    _handleCallback(callbackId: number, messageBuffer?: Buffer, streamScraps?: Buffer, transfer?: dataBeams.TransferIn) {
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
            var args: any[] = JSON.parse(argsString);
            if (transfer) {
                var inStreamHandler = new InStreamManager(transfer, streamScraps);
                // Loop through args and detect and inject streams
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
        } else {
            callback.call(this);
        }
    }

}

export function streamBeam(connection: dataBeams.Connection): StreamConnection {

    function buildCallbackHandler(callbackId: number): Function {
        return function streamBeamsCallbackResponder() {
            var outStreamManager: OutStreamManager;
            var transferLength = 5;
            var writeOffset = 0;

            var flags = StreamFlags.HasCallback | StreamFlags.IsCallback;

            // Hijack streams in the arguments
            // TODO: This is a lazy way, should create a proper array struct using buffers
            for (var i = 0; i < arguments.length; i++) {
                var arg = arguments[i];
                if (arg instanceof stream.Readable) {
                    outStreamManager = outStreamManager || new OutStreamManager();
                    arguments[i] = STREAM_STRING_LEAD + outStreamManager.addStream(arg);
                }
            }
            if (outStreamManager) {
                flags |= StreamFlags.HasStream;
            }

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
            } else {
                // Send just the buffer, there are no streams
                connection.sendBuffer(outBuffer);
            }
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
            // Handle as message stream push
            dbgStreamConnection('Checking if stream start satisfied, %d of %d bytes received', transferBuffer.length, streamStart);
            if (streamStart !== null && transferBuffer.length >= streamStart) {
                // Got all the data we needed to process, remove listeners.
                streamBeamsRemoveListeners();
                if (isCallback) {
                    var callbacks: { [id: number]: Function; };
                    var callback: Function;

                    dbgStreamConnection('Handling callback %d, transfer %d, argsBytes %d', callbackId, transfer.id, messageByteLength || 0);
                    if (hasMessage) {
                        if (hasStream) {
                            (<StreamConnection>connection)._handleCallback(
                                callbackId,
                                transferBuffer.slice(readOffset, streamStart),
                                transferBuffer.slice(streamStart),
                                transfer
                            );
                        } else {
                            (<StreamConnection>connection)._handleCallback(
                                callbackId,
                                transferBuffer.slice(readOffset, streamStart)
                            );
                        }
                    } else {
                        (<StreamConnection>connection)._handleCallback(callbackId);
                    }
                } else {
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
    (<StreamConnection>connection).Callbacks = {};

    return <StreamConnection>connection;
}
