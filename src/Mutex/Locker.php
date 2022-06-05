<?php

namespace Core\Mutex;

use Core\Logger\Logger;
use Core\Manticore\ManticoreConnector;
use mysqli;

class Locker
{
    private $fp;
    private $name;
    private $optimizeLockFile;

    public function __construct($name)
    {
        $this->fp = fopen(DIRECTORY_SEPARATOR.'tmp'.DIRECTORY_SEPARATOR.$name.'.lock', 'wb+');
        $this->name = $name;
        if (defined("OPTIMIZE_FILE")) {
            $this->optimizeLockFile = OPTIMIZE_FILE;
        }
    }

    public function checkLock(): bool
    {
        if (!flock($this->fp, LOCK_EX | LOCK_NB)) {
            Logger::log("Another process $this->name is already running");
            $this->unlock();
        }

        return true;
    }

    public function unlock($exitStatus = 1): void
    {
        fclose($this->fp);
        exit($exitStatus);
    }

    public function checkOptimizeLock($file): bool
    {
        if ($this->optimizeLockFile !== null && file_exists($this->optimizeLockFile)) {
            $ip = file_get_contents($file);

            if (!defined('WORKER_PORT')){
                throw new \RuntimeException("WORKER_PORT is not defined!");
            }
            $manticore = new ManticoreConnector($ip, WORKER_PORT, null, -1);
            $manticore->setMaxAttempts(180);
            $rows = $manticore->showThreads();

            if ($rows) {
                foreach ($rows as $row) {
                    if (strpos($row, 'SYSTEM OPTIMIZE') !== false) {
                        return true;
                    }
                }
            }

            unlink($file);
        }

        return false;
    }

    public function setOptimizeLock($ip): void
    {
        if ($this->optimizeLockFile === null) {
            throw new \http\Exception\RuntimeException("OPTIMIZE lock file is not set");
        }
        file_put_contents($this->optimizeLockFile, $ip);
    }
}
