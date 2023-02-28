<?php

declare(strict_types=1);

namespace FOD\DBALClickHouse\SocketStream;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use FOD\DBALClickHouse\ClickHouseException;

/**
 * ClickHouse connection
 */
class ClickHouseConnection extends \FOD\DBALClickHouse\ClickHouseConnection
{
    protected Configuration $configuration;

    /**
     * @var resource
     */
    protected $socket;

    protected ResponseParserInterface $responseParser;

    public function __construct(
        Configuration           $configuration,
        AbstractPlatform        $platform,
        ResponseParserInterface $responseParser
    )
    {
        $this->configuration  = $configuration;
        $this->platform       = $platform;
        $this->responseParser = $responseParser;

        $this->socket = fsockopen(
            $this->configuration->getHost(),
            (int)$this->configuration->getPort(),
            $errorCode,
            $errorMessage
        );

        if ($this->socket === false) {
            throw new ClickHouseException($errorMessage, $errorCode);
        }
    }

    public function __destruct()
    {
        if ($this->socket) {
            fclose($this->socket);
        }
    }

    public function prepare($sql)
    {
        return new ClickHouseStatement(
            $this->socket,
            $sql,
            $this->platform,
            $this->configuration,
            $this->responseParser
        );
    }

    public function getCurrentDatabase(): string
    {
        return $this->configuration->getDatabase();
    }
}
