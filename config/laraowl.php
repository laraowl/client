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
    //
    // The `driver` switch selects how records leave the application worker:
    //   - "http"  — default. Synchronous Guzzle POST inside the request
    //               lifecycle. Simple, no extra infra, but blocks the worker
    //               for up to `timeout` seconds per flush and can amplify
    //               latency under load (see README "Async ingest mode").
    //   - "redis" — push records to a Redis stream and let `php artisan
    //               laraowl:work` forward them out-of-band. Worker stays
    //               unblocked even when the LaraOwl server is slow.
    //   - "null"  — drop every record. Useful for load tests, ephemeral
    //               containers, and forensic comparisons.
    'ingest' => [
        'driver' => env('LARAOWL_INGEST_DRIVER', 'http'),
        'timeout' => (float) env('LARAOWL_INGEST_TIMEOUT', 2.0),
        'buffer_size' => (int) env('LARAOWL_INGEST_BUFFER', 500),

        // Settings used when driver=redis. The sidecar consumes the same
        // stream/connection via `laraowl:work`.
        'redis' => [
            'connection' => env('LARAOWL_INGEST_REDIS_CONNECTION', 'default'),
            'stream' => env('LARAOWL_INGEST_REDIS_STREAM', 'laraowl:ingest'),
            // Either an int (MAXLEN entries to retain) or a duration string
            // accepted by CarbonInterval::fromString (MINID by-age trim).
            'trim_keep' => env('LARAOWL_INGEST_REDIS_TRIM_KEEP', '5 minutes'),
        ],
    ],
];
