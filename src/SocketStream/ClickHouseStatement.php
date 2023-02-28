<?php

declare(strict_types=1);

namespace FOD\DBALClickHouse\SocketStream;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use FOD\DBALClickHouse\ClickHouseException;
use PDO;

/**
 * ClickHouse Statement
 */
class ClickHouseStatement extends \FOD\DBALClickHouse\ClickHouseStatement
{
    protected const READ_BYTES = 65535;

    /**
     * @var resource
     */
    protected $socket;

    protected Configuration $configuration;

    protected ResponseParserInterface $responseParser;

    public function __construct(
        $socket,
        string $statement,
        AbstractPlatform $platform,
        Configuration $configuration,
        ResponseParserInterface $responseParser
    )
    {
        $this->socket         = $socket;
        $this->statement      = $statement;
        $this->platform       = $platform;
        $this->configuration  = $configuration;
        $this->responseParser = $responseParser;
    }

    protected function executeSql(string $sql): void
    {
        $rawHttpMessage = new ClickHouseRawHttpMessage($sql, $this->configuration);

        fwrite($this->socket, (string)$rawHttpMessage, $rawHttpMessage->length());
    }

    public function fetch($fetchMode = null, $cursorOrientation = PDO::FETCH_ORI_NEXT, $cursorOffset = 0): \Generator
    {
        $processingBodyStarted = false;

        while (($line = stream_get_line($this->socket, self::READ_BYTES, "\r\n")) !== false) {
            if (empty($line)) {
                // received headers delimiter
                $processingBodyStarted = true;
            } elseif (!$processingBodyStarted && strpos($line, 'X-ClickHouse-Exception') !== false) {
                // received error response from server
                throw new ClickHouseServerException($line);
            } elseif ($processingBodyStarted) {
                // received data
                $block = stream_get_line($this->socket, self::READ_BYTES, "\r\n");

                if (strpos($block, 'DB::Exception') !== false) {
                    throw new ClickHouseDBException($block);
                }

                yield from $this->responseParser->add($block)->row();
            }
        }
    }

    public function fetchAll($fetchMode = null, $fetchArgument = null, $ctorArgs = null)
    {
        throw new ClickHouseException('You need to implement ClickHouseStatement::' . __METHOD__ . '()');
    }

    public function fetchColumn($columnIndex = 0)
    {
        throw new ClickHouseException('You need to implement ClickHouseStatement::' . __METHOD__ . '()');
    }
}
