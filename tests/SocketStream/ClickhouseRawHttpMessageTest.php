<?php

namespace FOD\DBALClickHouse\Tests\SocketStream;

use FOD\DBALClickHouse\ClickHouseException;
use FOD\DBALClickHouse\SocketStream\ClickHouseRawHttpMessage;
use FOD\DBALClickHouse\SocketStream\Configuration;
use PHPUnit\Framework\TestCase;

class ClickhouseRawHttpMessageTest extends TestCase
{
    /**
     * @dataProvider connectDataProvider
     */
    public function testInit($params, $username, $password, $sql)
    {
        if (empty($sql)) {
            $this->expectException(ClickHouseException::class);
        }
        $configuration = new Configuration($params, $username, $password);

        $sut = new ClickHouseRawHttpMessage($sql, $configuration);
        $this->assertGreaterThan(0, $sut->length());

        $result = (string)$sut;

        $this->assertNotFalse(strpos($result, 'POST /?database=' . ($params['dbname'] ?? 'default')));
        $this->assertNotFalse(strpos($result, ($params['host'] ?? 'localhost') . ':' . ($params['port'] ?? '8123')));
        $this->assertNotFalse(strpos($result, 'Content-Type: application/x-www-form-urlencoded'));

        if ($username) {
            $this->assertNotFalse(strpos($result, 'X-Clickhouse-User: ' . $username));

            if ($password) {
                $this->assertNotFalse(strpos($result, 'X-Clickhouse-Key: ' . $password));
            } else {
                $this->assertFalse(strpos($result, 'X-Clickhouse-Key'));
            }
        } else {
            $this->assertFalse(strpos($result, 'X-Clickhouse-User'));
        }

        $this->assertNotFalse(strpos($result, $sql . ' SETTINGS max_execution_time = 0'));
    }

    public function connectDataProvider(): \Generator
    {
        yield [
            [
                'host'   => '1.2.3.4',
                'port'   => 8123,
                'dbname' => 'test',
            ],
            'testuser',
            'testpassword',
            'SELECT * FROM users',
        ];

        yield [
            [],
            'testuser',
            'testpassword',
            'SELECT * FROM users',
        ];

        yield [
            [
                'host'   => '1.2.3.4',
                'port'   => 8123,
                'dbname' => 'test',
            ],
            '',
            '',
            'SELECT * FROM users',
        ];

        yield [
            [
                'host'   => '1.2.3.4',
                'port'   => 8123,
                'dbname' => 'test',
            ],
            'testuser',
            'testpassword',
            '',
        ];
    }
}
