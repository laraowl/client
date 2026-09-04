<?php

return [

    // Enable or disable monitoring
    'enabled' => env('LARAOWL_ENABLED', true),

    // Application Token and Server URL
    'token' => env('LARAOWL_TOKEN'),
    'server_url' => env('LARAOWL_SERVER_URL', 'https://laraowl.test'),

    // Metadata about the environment
    'environment' => [
        'deploy_id' => env('LARAOWL_DEPLOY'),
        'server_name' => env('LARAOWL_SERVER_NAME', gethostname()),
    ],

    // Privacy and Data Redaction
    'privacy' => [
        'capture_source_code' => env('LARAOWL_CAPTURE_SOURCE_CODE', true),
        'capture_payload' => env('LARAOWL_CAPTURE_PAYLOAD', false),
        'redact_fields' => explode(',', env('LARAOWL_REDACT_FIELDS', '_token,password,password_confirmation')),
        'redact_headers' => explode(',', env('LARAOWL_REDACT_HEADERS', 'Authorization,Cookie,Proxy-Authorization,X-XSRF-TOKEN')),
    ],

    // Sampling Rates (1.0 = capture everything)
    'sampling' => [
        'requests' => (float) env('LARAOWL_SAMPLE_REQUESTS', 1.0),
        'commands' => (float) env('LARAOWL_SAMPLE_COMMANDS', 1.0),
        'exceptions' => (float) env('LARAOWL_SAMPLE_EXCEPTIONS', 1.0),
        'tasks' => (float) env('LARAOWL_SAMPLE_TASKS', 1.0),
    ],

    // What to ignore
    'ignore' => [
        'cache' => env('LARAOWL_IGNORE_CACHE', false),
        'mail' => env('LARAOWL_IGNORE_MAIL', false),
        'notifications' => env('LARAOWL_IGNORE_NOTIFICATIONS', false),
        'queries' => env('LARAOWL_IGNORE_QUERIES', false),
        'outgoing_requests' => env('LARAOWL_IGNORE_OUTGOING', false),
    ],

    // Transmission Settings
    'ingest' => [
        'timeout' => (float) env('LARAOWL_INGEST_TIMEOUT', 2.0),
        'buffer_size' => (int) env('LARAOWL_INGEST_BUFFER', 500),
    ],

    // Queue Settings
    //
    // When a connection is set, the HTTP POST to the Laraowl server is
    // dispatched to a queued job instead of blocking the request/command while
    // it completes. Leave LARAOWL_QUEUE_CONNECTION empty to keep the current
    // synchronous behavior; LARAOWL_QUEUE only names which queue to use once
    // queueing is enabled.
    //
    // LARAOWL_QUEUE defaults to null, meaning the connection's own default
    // queue -- so setting LARAOWL_QUEUE_CONNECTION alone is enough and a
    // stock `php artisan queue:work <connection>` picks the batches up. Name
    // a queue here only if you also run a worker listening on it, otherwise
    // the batches pile up unprocessed and telemetry stops with no error.
    'queue' => [
        'connection' => env('LARAOWL_QUEUE_CONNECTION'),
        'queue' => env('LARAOWL_QUEUE'),
        'delay' => (int) env('LARAOWL_QUEUE_DELAY', 0),
    ],
];
