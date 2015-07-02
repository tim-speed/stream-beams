/// <reference path="../lib/node.d.ts" />
/// <reference path="../node_modules/data-beams/data-beams.d.ts" />
import dataBeams = require('data-beams');
import stream = require('stream');
export declare class WrappedStream extends dataBeams.ArrayBufferedStream {
    constructor(stream: stream.Readable, dataPipeHandler?: (data: Buffer) => void);
}
export declare class HeaderWrappedStream extends WrappedStream {
    constructor(stream: stream.Readable, header: NodeBuffer);
}
export declare class StreamConnection extends dataBeams.Connection {
    Callbacks: {
        [id: number]: Function;
    };
    stream(message: Object, stream?: stream.Readable, callback?: Function): dataBeams.TransferOut;
    _handleCallback(callbackId: number, messageBuffer?: Buffer, streamScraps?: Buffer, transfer?: dataBeams.TransferIn): void;
}
export declare function streamBeam(connection: dataBeams.Connection): StreamConnection;
