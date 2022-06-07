<?php

namespace Core\Logger;

class Logger
{
    public static function log(string $str)
    {
        echo "==>\t$str\n";
    }
}
