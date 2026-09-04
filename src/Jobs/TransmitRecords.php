<?php

namespace Laraowl\Client\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Laraowl\Client\Core;
use Laraowl\Client\State\CommandState;
use Laraowl\Client\State\RequestState;

/**
 * Sends an already-bufferred batch of records to the Laraowl server from a
 * queue worker instead of the original request/command, mirroring how
 * laravel/telescope defers `ProcessPendingUpdates` to the queue.
 *
 * @internal
 */
final class TransmitRecords implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * @param  array<int, array<mixed>>  $records
     */
    public function __construct(
        private array $records,
    ) {
        //
    }

    /**
     * @param  Core<RequestState|CommandState>  $core
     */
    public function handle(Core $core): void
    {
        // Nothing this job does is application traffic, so suppress capture
        // for the whole transmit: the batch we are sending must never be
        // able to seed the next one. Concretely, transmitBatch() logs via
        // Log::error when the POST fails, which becomes a `log` record in
        // any app that routes the laraowl channel back through the package
        // -- one failed POST would then queue a job forever. HttpIngest
        // already marks the payload unsampled; this closes the same door
        // from the worker side.
        $core->ignore(fn () => $core->ingest->transmitBatch($this->records));
    }
}
