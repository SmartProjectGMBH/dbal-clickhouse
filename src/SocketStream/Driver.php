<?php

declare(strict_types=1);

namespace FOD\DBALClickHouse\SocketStream;

use Doctrine\DBAL\Connection;
use FOD\DBALClickHouse\ClickHousePlatform;
use FOD\DBALClickHouse\ClickHouseSchemaManager;

/**
 * ClickHouse Driver
 */
class Driver implements \Doctrine\DBAL\Driver
{
    public function connect(array $params, $username = null, $password = null, array $driverOptions = [])
    {
        return new ClickHouseConnection(
            new Configuration($params, $username, $password),
            $this->getDatabasePlatform(),
            $this->getResponseParserClass($params)
        );
    }

    public function getDatabasePlatform()
    {
        return new ClickHousePlatform();
    }

    public function getSchemaManager(Connection $conn)
    {
        return new ClickHouseSchemaManager($conn);
    }

    public function getName()
    {
        return 'clickhouse';
    }

    public function getDatabase(Connection $conn)
    {
        return $conn->getParams()['dbname'] ?? 'default';
    }

    protected function getResponseParserClass(array $params): ResponseParserInterface
    {
        $className = $params['responseParserClass']
            ?? '\FOD\DBALClickHouse\SocketStream\JSONEachRowStreamResponseParser';

        $result = new $className();

        if (!($result instanceof ResponseParserInterface)) {
            throw new \LogicException('CH Response parser class must implement ResponseParserInterface');
        }

        return $result;
    }
}
