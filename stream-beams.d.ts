declare module "stream-beams" {

    import dataBeams = require('data-beams');
    import stream = require('stream');
    export class WrappedStream extends dataBeams.ArrayBufferedStream {
        constructor(stream: stream.Readable, dataPipeHandler?: (data: Buffer) => void);
    }
    export class HeaderWrappedStream extends WrappedStream {
        constructor(stream: stream.Readable, header: NodeBuffer);
    }
    export class StreamConnection extends dataBeams.Connection {
        Callbacks: {
            [id: number]: Function;
        };
        stream(message: Object, stream?: stream.Readable, callback?: Function): dataBeams.TransferOut;
        _handleCallback(callbackId: number, messageBuffer?: Buffer, streamScraps?: Buffer, transfer?: dataBeams.TransferIn): void;
    }
    export function streamBeam(connection: dataBeams.Connection): StreamConnection;

}
