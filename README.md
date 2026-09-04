# Laraowl Client for Laravel

Laraowl is a self-hosted monitoring solution for Laravel applications. This package allows you to collect real-time metrics, exceptions, and performance data from your applications and send them to your Laraowl Server.

## Features

- **Exception Tracking**: Detailed reports on errors with stack traces and request data.
- **Performance Monitoring**: Track database queries, jobs, and execution times.
- **Service Integration**: Built-in support for Mail, Notifications, and Cache monitoring.
- **Zero Configuration**: Sensible defaults that work out of the box.

## Installation

Install the package via Composer:

```bash
composer require laraowl/client
```

## Configuration

### 1. Run the Install Command

Laraowl provides a convenient installation command to set up the configuration and environment variables:

```bash
php artisan laraowl:install
```

This command will:
- Publish the `laraowl.php` configuration file.
- Prompt you for your Laraowl Server URL and Project Token.
- Automatically update your `.env` file.


### 2. Environment Setup

Add your Laraowl Server details to your `.env` file:

```env
LARAOWL_SERVER_URL=https://your-laraowl-server.com
LARAOWL_TOKEN=your-project-token
```

### 3. Queued Delivery (optional)

By default Laraowl POSTs each buffered batch to your server synchronously,
while the request or command is terminating. Set a queue connection to hand
that POST to a queue worker instead, so the Laraowl server's latency never
counts against your own response time:

```env
LARAOWL_QUEUE_CONNECTION=redis
```

| Variable | Default | Purpose |
| --- | --- | --- |
| `LARAOWL_QUEUE_CONNECTION` | *(empty)* | Queue connection used to deliver batches. Empty keeps the synchronous behavior — queueing is strictly opt-in. |
| `LARAOWL_QUEUE` | *(empty)* | Queue name. Empty means the connection's default queue, which is what a stock `php artisan queue:work <connection>` consumes. Set this only if you also run a worker listening on that queue. |
| `LARAOWL_QUEUE_DELAY` | `0` | Seconds to delay each batch. |

Batches delivered this way are excluded from your own telemetry, so the
delivery jobs never show up as monitored traffic.

## License

Laraowl Client is open-source software licensed under the [MIT license](LICENSE.md).

