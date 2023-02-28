<?php

namespace FOD\DBALClickHouse\SocketStream;

/**
 * Converts clickhouse DB error response code into human-readable message
 */
class ClickHouseDBException extends ClickHouseServerException
{
    protected function parseRawException(string $message): array
    {
        [$code, $originalMessage] = explode('. ', $message, 2);
        $code = (int)str_replace('Code: ', '', $code);

        return [
            'code'    => $code,
            'message' => (static::CH_SERVER_CODES[$code] ?? '') . ' (' . $originalMessage . ')',
        ];
    }

}