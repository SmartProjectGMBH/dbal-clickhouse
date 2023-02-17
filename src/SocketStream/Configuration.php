<?php

declare(strict_types=1);

namespace FOD\DBALClickHouse\SocketStream;

/**
 * Connection configuration
 */
final class Configuration extends \Doctrine\DBAL\Configuration
{
    public function __construct(
        array  $params,
        string $username = null,
        string $password = null,
        array  $serverOptions = null
    )
    {
        $this->_attributes['scheme']   = ($params['driverOptions']['https'] ?? false) ? 'http' : 'https';
        $this->_attributes['host']     = $params['host'] ?? 'localhost';
        $this->_attributes['port']     = $params['port'] ?? '8123';
        $this->_attributes['username'] = $username ?? '';
        $this->_attributes['password'] = $password ?? '';

        $this->_attributes['server'] = array_replace([
            'database'                       => $params['dbname'] ?? 'default',
            'default_format'                 => 'JSONEachRow',
            // 'enable_http_compression'       => 1,
            // 'max_result_rows'               => 10000,
            // 'max_result_bytes'              => 10000000,
            'buffer_size'                    => 4096,
            'wait_end_of_query'              => 0,
            'send_progress_in_http_headers'  => 0,
            'output_format_enable_streaming' => 1,
            'result_overflow_mode'           => 'break',
        ], $serverOptions ?? []);
    }

    public function getScheme(): string
    {
        return $this->_attributes['scheme'];
    }

    public function getHost(): string
    {
        return $this->_attributes['host'];
    }

    public function getPort(): string
    {
        return $this->_attributes['port'];
    }

    public function getUsername(): string
    {
        return $this->_attributes['username'];
    }

    public function getPassword(): string
    {
        return $this->_attributes['password'];
    }

    public function getDatabase(): string
    {
        return $this->_attributes['server']['database'];
    }

    public function getServerConnectionParams()
    {
        return '?' . http_build_query($this->_attributes['server']);
    }
}
