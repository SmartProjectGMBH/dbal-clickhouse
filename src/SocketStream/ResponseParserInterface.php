<?php

namespace FOD\DBALClickHouse\SocketStream;

use Generator;

/**
 * ClickHouse server response parser general interface
 */
interface ResponseParserInterface
{
    public function add(string $block) : self;

    public function row(): Generator;
}
