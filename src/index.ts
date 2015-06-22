/// <reference path="../lib/node.d.ts" />
/// <reference path="../node_modules/data-beams/data-beams.d.ts" />

import dataBeams = require('data-beams');
import stream = require('stream');

import debug = require('./debug');

export class HeaderWrappedStream extends stream.Readable {

    _stream: stream.Readable;
    _header: NodeBuffer;
    _paused: boolean;
    _dataHandler: (data: NodeBuffer) => void;

    constructor(stream: stream.Readable, header: NodeBuffer) {
        super();

        this._paused = true;
        stream.pause();
        this._stream = stream;
        this._header = header;
        var _ = this;
        // Store data handler method, added on resume
        this._dataHandler = function headerWrappedStreamDataHandler(data: NodeBuffer) {
            // This is where we pipe data packets
            _.emit('data', data);
        };
        // Bubble up events
        stream.once('end', function headerWrappedStreamEndHandler() {
            _._stream.removeListener('data', _._dataHandler);
            _.emit('end');
        });
    }

    read(length: number): NodeBuffer {
        throw new Error("Can not read synchronously, use data events and pause / resume.");
    }
    setEncoding(encoding: string): HeaderWrappedStream {
        if (encoding)
            throw new Error("Can not set encoding of header wrapped stream! It is designed for binary only.");
        return this;
    }
    resume(): HeaderWrappedStream {
        this._paused = false;

        if (this._header) {
            // Send the header the first time
            this.emit('data', this._header);
            this._header = null;
        }

        this._stream.on('data', this._dataHandler);
        this._stream.resume();

        return this;
    }
    pause(): HeaderWrappedStream {
        this._paused = true;
        this._stream.pause();
        this._stream.removeListener('data', this._dataHandler);

        return this;
    }
    isPaused(): boolean {
        return this._paused;
    }
    pipe(destination: stream.Writable, options?: { end: boolean }): void {
        throw new Error("Not implemented on HeaderWrappedStream");
    }
    unpipe(destination?: stream.Writable): void {
        throw new Error("Not implemented on HeaderWrappedStream");
    }
    unshift(chunk: any): void {
        // Accepts Buffer or String
        throw new Error("Not implemented on HeaderWrappedStream");
    }
    wrap(stream: stream.Readable): stream.Readable {
        throw new Error("Not implemented on HeaderWrappedStream");
    }

}

var dbgStreamConnection = debug('StreamConnection');

export class StreamConnection extends dataBeams.Connection {

    stream(message: Object, stream?: stream.Readable): dataBeams.TransferOut {
        var hasMessage = !!message;
        var outBuffer: NodeBuffer;
        if (hasMessage) {
            var outMessage = JSON.stringify(message);
            var byteLength = Buffer.byteLength(outMessage, 'utf8');
            outBuffer = new Buffer(byteLength + 4);
            outBuffer.writeUInt32BE(byteLength, 0);
            outBuffer.write(outMessage, 4, byteLength, 'utf8');
        } else {
            outBuffer = new Buffer(4);
            outBuffer.writeUInt32BE(0, 0);
        }
        if (stream) {
            // TODO: Infinites are not supported yet, they still need to be worked into the underlying data-beams framework
            // TODO: Also consider replacing header wrapped stream with a custom Duplex or PassThrough
            return this.sendStream(new HeaderWrappedStream(stream, outBuffer));
        } else {
            return this.sendBuffer(outBuffer);
        }
    }

}

function StreamifyTransfer(transfer: dataBeams.TransferIn, intialBuffer?: NodeBuffer): stream.Readable {
    var rs = new stream.Readable();
    if (intialBuffer) {
        rs.push(intialBuffer);
    }
    function streamifiedTransferDataHandler(data: NodeBuffer) {
        rs.push(data);
    }
    transfer.on('data', streamifiedTransferDataHandler);
    transfer.once('complete', function streamifiedTransferCompleteHandler() {
        transfer.removeListener('data', streamifiedTransferDataHandler);
    });
    return rs;
}

export function streamBeam(connection: dataBeams.Connection): StreamConnection {

    function streamBeamsOnTransfer(transfer: dataBeams.TransferIn) {
        // Handle transfers through the data-beams connection
        var messageLength: number = null;
        var transferBuffer: NodeBuffer = null;
        // Handle the data of the transfer
        function streamBeamsHandleTransfer(data: NodeBuffer) {
            // Concat our buffer until we resolve out JSON message and stream
            if (!transferBuffer)
                transferBuffer = data;
            else
                transferBuffer = Buffer.concat([transferBuffer, data]);

            if (messageLength === null && transferBuffer.length >= 4) {
                // We can read from zero because this has been sliced
                messageLength = transferBuffer.readUInt32BE(0);
            }

            var messageOffset;
            if (messageLength !== null && (messageOffset = messageLength + 4) && transferBuffer.length >= messageOffset) {
                dbgStreamConnection('Parsing message from transfer %d, length %d', transfer.id, messageLength);
                // Remove this as a data listener pre-maturely cause we don't need it anymore
                streamBeamsRemoveListeners();
                // Process the message object
                var messageObj = null;
                if (messageLength) {
                    var messageString = transferBuffer.toString('utf8', 4, messageOffset);
                    messageObj = JSON.parse(messageString);
                }
                // TODO: Will need to think about cleanups and timeouts for infinite streams...
                //       Also should maybe add some logic to abandon transfers after a disconnect if no new data is passed through within a timeout time
                if (messageOffset === transferBuffer.length && transfer.complete) {
                    // Emit just the message, there is no data stream
                    connection.emit('message', messageObj);
                } else {
                    // Emit the message and stream the rest, supplying the length if it is not infinite
                    connection.emit('message', messageObj, StreamifyTransfer(transfer, transferBuffer.slice(messageOffset)));
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

    return <StreamConnection>connection;
}
