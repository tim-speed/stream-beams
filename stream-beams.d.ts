declare module "stream-beams" {

    import dataBeams = require('data-beams');
    import stream = require('stream');

    export class HeaderWrappedStream extends stream.Readable {
        _stream: stream.Readable;
        _header: NodeBuffer;
        _paused: boolean;
        _dataHandler: (data: NodeBuffer) => void;
        constructor(stream: stream.Readable, header: NodeBuffer);
        read(length: number): NodeBuffer;
        setEncoding(encoding: string): HeaderWrappedStream;
        resume(): HeaderWrappedStream;
        pause(): HeaderWrappedStream;
        isPaused(): boolean;
        pipe(destination: stream.Writable, options?: {
            end: boolean;
        }): void;
        unpipe(destination?: stream.Writable): void;
        unshift(chunk: any): void;
        wrap(stream: stream.Readable): stream.Readable;
    }

    export class StreamConnection extends dataBeams.Connection {
        stream(message: Object, stream?: stream.Readable): dataBeams.TransferOut;
    }

    export function streamBeam(connection: dataBeams.Connection): StreamConnection;

}
