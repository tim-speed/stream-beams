/// <reference path="../lib/node.d.ts" />
/// <reference path="../node_modules/data-beams/data-beams.d.ts" />
import dataBeams = require('data-beams');
import stream = require('stream');
export declare class HeaderWrappedStream extends stream.Readable {
    _ended: boolean;
    _received: number;
    _push: (chunk: any, encoding?: string) => boolean;
    _data: NodeBuffer[];
    _readLimit: number;
    end: () => void;
    constructor(stream: stream.Readable, header?: NodeBuffer);
    _checkEnd(): boolean;
    _sendData(): void;
    _read(size: number): void;
}
export declare class StreamConnection extends dataBeams.Connection {
    Callbacks: {
        [id: number]: Function;
    };
    stream(message: Object, stream?: stream.Readable, callback?: Function): dataBeams.TransferOut;
}
export declare function streamBeam(connection: dataBeams.Connection): StreamConnection;
