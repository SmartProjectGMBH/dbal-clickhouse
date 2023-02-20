<?php

namespace FOD\DBALClickHouse\SocketStream;

use FOD\DBALClickHouse\ClickHouseException;

/**
 * ClickHouse raw Http message
 */
class ClickHouseRawHttpMessage
{
    protected string $message;

    public function __construct(string $sql, Configuration $configuration)
    {
        if (empty($sql)) {
            throw new ClickHouseException('Empty SQL');
        }

        $sql .= ' SETTINGS max_execution_time = 0';

        $headers = [
            "Accept-Language: en-GB,en-US,q=0.9,en,q=0.8",
            "Connection: keep-alive",
            "Content-Type: application/x-www-form-urlencoded",
        ];

        if ($configuration->getUsername()) {
            $headers[] = "X-Clickhouse-User: " . $configuration->getUsername();

            if ($configuration->getPassword()) {
                $headers[] = "X-Clickhouse-Key: " . $configuration->getPassword();
            }
        }

        $this->message = "POST /" . $configuration->getServerConnectionParams() . " HTTP/1.1" . "\r\n";
        $this->message .= "Host: " . $configuration->getHost() . ":" . $configuration->getPort() . "\r\n";
        $this->message .= implode("\r\n", $headers);
        $this->message .= "\r\n";
        $this->message .= "Content-Length: " . strlen($sql) . "\r\n";
        $this->message .= "Connection: Close\r\n\r\n";
        $this->message .= $sql;
    }

    public function __toString(): string
    {
        return $this->message;
    }

    public function length(): int
    {
        return strlen($this->message);
    }
}
